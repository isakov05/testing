"""
Factoring Risk Engine - Core Module

This module implements a production-ready factoring/supply chain finance risk engine
that calculates PD (Probability of Default), LGD (Loss Given Default), EAD (Exposure At Default),
Expected Loss, and recommended credit limits per counterparty.

Architecture:
- RiskEngine: Main orchestrator class
- BehavioralFeatureEngine: Feature calculation
- PDScoringModel: PD calculation and rating assignment
- LGDEngine: Loss Given Default calculation
- EADEngine: Exposure At Default calculation
- CreditLimitEngine: Credit limit recommendations
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

from utils.risk_utils import (
    parse_return_invoice_id,
    is_return_document,
    calculate_dpd,
    assign_aging_bucket,
    load_risk_config,
    decimal_to_float,
    calculate_coefficient_of_variation,
    parse_contract_number_from_payment
)


class RiskEngine:
    """
    Main risk engine orchestrator that reconstructs payment behavior
    from invoices and bank transactions.
    """
    
    def __init__(self, config: Optional[Dict] = None, user_id: str = None):
        """
        Initialize Risk Engine.
        
        Args:
            config: Optional configuration dictionary
            user_id: User/company identifier
        """
        self.config = config or load_risk_config()
        self.user_id = user_id
        self.invoice_components = []
        self.counterparty_features = {}
        
    def reconstruct_invoice_components(
        self,
        invoices_df: pd.DataFrame,
        payments_df: pd.DataFrame,
        invoice_type: str = 'OUT',
        as_of_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """
        Reconstruct invoice components (paid, returned, open) with FIFO payment allocation.
        
        Args:
            invoices_df: DataFrame with invoice data
            payments_df: DataFrame with bank transaction data
            invoice_type: 'OUT' for AR (receivables), 'IN' for AP (payables)
            as_of_date: Analysis cutoff date (defaults to today)
            
        Returns:
            List of invoice component dictionaries
        """
        if as_of_date is None:
            as_of_date = date.today()
        elif isinstance(as_of_date, datetime):
            as_of_date = as_of_date.date()
        
        components = []
        
        if invoices_df.empty:
            return components
        
        # Prepare invoices dataframe
        inv_df = invoices_df.copy()
        
        # Standardize column names
        inv_df = self._standardize_invoice_columns(inv_df, invoice_type)
        
        # Remove TRUE duplicates only (exact same document_number, date, AND amount)
        # Keep records with same doc# but different amounts (these are separate line items/invoices)
        inv_df = inv_df.drop_duplicates(
            subset=['document_number', 'document_date', 'total_amount'],
            keep='first'
        ).reset_index(drop=True)
        
        # Exclude cancelled returns (status = 'Отменен')
        if 'status' in inv_df.columns:
            inv_df = inv_df[inv_df['status'] != 'Отменен'].reset_index(drop=True)
        
        # Separate returns from regular invoices
        returns_df = inv_df[inv_df['is_return'] == True].copy()
        regular_invoices_df = inv_df[inv_df['is_return'] == False].copy()

        # Process by counterparty
        counterparty_col = 'buyer_inn' if invoice_type == 'OUT' else 'seller_inn'

        # Get ALL unique counterparties (from both regular invoices AND returns)
        # This ensures we process counterparties that only have returns
        all_counterparty_inns = set()
        if not regular_invoices_df.empty:
            all_counterparty_inns.update(regular_invoices_df[counterparty_col].dropna().unique())
        if not returns_df.empty:
            all_counterparty_inns.update(returns_df[counterparty_col].dropna().unique())

        for counterparty_inn in all_counterparty_inns:
            # Get invoices for this counterparty
            cp_invoices = regular_invoices_df[
                regular_invoices_df[counterparty_col] == counterparty_inn
            ].copy()
            
            # Get payments for this counterparty
            cp_payments = self._get_counterparty_payments(
                payments_df, 
                counterparty_inn, 
                invoice_type
            )
            
            # Get returns for this counterparty
            cp_returns = returns_df[
                returns_df[counterparty_col] == counterparty_inn
            ].copy()
            
            # Allocate payments using FIFO
            cp_components = self._allocate_payments_fifo(
                cp_invoices,
                cp_payments,
                cp_returns,
                as_of_date,
                counterparty_inn
            )
            
            components.extend(cp_components)
        
        self.invoice_components = components
        return components
    
    def _standardize_invoice_columns(
        self, 
        df: pd.DataFrame, 
        invoice_type: str
    ) -> pd.DataFrame:
        """
        Standardize invoice column names for processing.
        
        Args:
            df: Invoice DataFrame
            invoice_type: 'OUT' or 'IN'
            
        Returns:
            Standardized DataFrame
        """
        df = df.copy()
        
        # Map common column variations
        col_mapping = {
            'Document Number': 'document_number',
            'Номер документ': 'document_number',
            'Document Date': 'document_date',
            'Дата документ': 'document_date',
            'Supply Value (incl. VAT)': 'total_amount',
            'Стоимость поставки с учётом НДС': 'total_amount',
            'Buyer (Tax ID or PINFL)': 'buyer_inn',
            'Покупатель (ИНН или ПИНФЛ)': 'buyer_inn',
            'Buyer (Name)': 'buyer_name',
            'Покупатель (наименование)': 'buyer_name',
            'Seller (Tax ID or PINFL)': 'seller_inn',
            'Продавец (ИНН или ПИНФЛ)': 'seller_inn',
            'Seller (Name)': 'seller_name',
            'Продавец (наименование)': 'seller_name',
            'Contract Number': 'contract_number',
            'Номер договора': 'contract_number',
        }
        
        # Only rename if target doesn't already exist, and each target is claimed once
        safe_rename = {}
        claimed = set(df.columns)
        for src, tgt in col_mapping.items():
            if src in df.columns and tgt not in claimed:
                safe_rename[src] = tgt
                claimed.add(tgt)
        df = df.rename(columns=safe_rename)
        # Drop any remaining duplicate column names
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        # Ensure contract_number column exists
        if 'contract_number' not in df.columns:
            df['contract_number'] = ''
        
        # Ensure required columns exist
        if 'document_number' not in df.columns:
            df['document_number'] = df.index.astype(str)
        
        if 'document_date' not in df.columns:
            df['document_date'] = pd.NaT
        else:
            df['document_date'] = pd.to_datetime(df['document_date'], errors='coerce')
        
        if 'total_amount' not in df.columns:
            df['total_amount'] = 0
        else:
            df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
        
        # Identify returns
        df['is_return'] = df['document_number'].apply(is_return_document)
        
        # Clean INN fields
        for inn_col in ['buyer_inn', 'seller_inn']:
            if inn_col in df.columns:
                df[inn_col] = df[inn_col].astype(str).str.replace('.0', '', regex=False).str.strip()
        
        return df
    
    def _get_counterparty_payments(
        self,
        payments_df: pd.DataFrame,
        counterparty_inn: str,
        invoice_type: str
    ) -> pd.DataFrame:
        """
        Extract payments for a specific counterparty.
        
        Args:
            payments_df: Bank transactions DataFrame
            counterparty_inn: Counterparty INN
            invoice_type: 'OUT' or 'IN'
            
        Returns:
            Filtered payments DataFrame
        """
        if payments_df.empty:
            return pd.DataFrame()
        
        pay_df = payments_df.copy()
        
        # Standardize columns
        col_mapping = {
            'Taxpayer ID (INN)': 'counterparty_inn',
            'inn': 'counterparty_inn',
            'date': 'transaction_date',
            'Document Date': 'transaction_date',
            'Amount': 'amount',
            'amount': 'amount',
            'Transaction Type': 'transaction_type',
            'Payment Purpose': 'payment_purpose',
            'Назначение платежа': 'payment_purpose'
        }
        safe_rename = {}
        claimed = set(pay_df.columns)
        for src, tgt in col_mapping.items():
            if src in pay_df.columns and tgt not in claimed:
                safe_rename[src] = tgt
                claimed.add(tgt)
        pay_df = pay_df.rename(columns=safe_rename)
        pay_df = pay_df.loc[:, ~pay_df.columns.duplicated(keep='first')]

        # Ensure payment_purpose column exists
        if 'payment_purpose' not in pay_df.columns:
            pay_df['payment_purpose'] = ''
        
        # Clean INN
        if 'counterparty_inn' in pay_df.columns:
            pay_df['counterparty_inn'] = pay_df['counterparty_inn'].astype(str).str.replace('.0', '', regex=False).str.strip()
        
        # Filter by counterparty
        if 'counterparty_inn' in pay_df.columns:
            pay_df = pay_df[pay_df['counterparty_inn'] == counterparty_inn]
        
        # Filter by transaction type
        if 'transaction_type' in pay_df.columns:
            if invoice_type == 'OUT':
                # For receivables, we want incoming payments (customer pays us)
                pay_df = pay_df[pay_df['transaction_type'] == 'Incoming']
            else:
                # For payables, we want outgoing payments (we pay supplier)
                pay_df = pay_df[pay_df['transaction_type'] == 'Outgoing']
        
        # Ensure date and amount columns
        if 'transaction_date' in pay_df.columns:
            pay_df['transaction_date'] = pd.to_datetime(pay_df['transaction_date'], errors='coerce')
        else:
            pay_df['transaction_date'] = pd.NaT
        
        if 'amount' not in pay_df.columns:
            pay_df['amount'] = 0
        else:
            pay_df['amount'] = pd.to_numeric(pay_df['amount'], errors='coerce').fillna(0)
        
        return pay_df
    
    def _allocate_payments_fifo(
        self,
        invoices: pd.DataFrame,
        payments: pd.DataFrame,
        returns: pd.DataFrame,
        as_of_date: date,
        counterparty_inn: str
    ) -> List[Dict[str, Any]]:
        """
        Allocate payments to invoices using contract-based matching first, then FIFO.
        
        Strategy:
        1. Parse contract numbers from payment_purpose (after "Договор №")
        2. Match payments to invoices by contract_number first
        3. Allocate remaining payments using FIFO by date
        
        Args:
            invoices: Invoices for this counterparty
            payments: Payments from this counterparty
            returns: Returns for this counterparty
            as_of_date: Analysis cutoff date
            counterparty_inn: Counterparty INN
            
        Returns:
            List of invoice components
        """
        components = []
        
        # Remove TRUE duplicates only (same doc#, date, AND amount)
        # NOTE: Do NOT deduplicate invoices with same doc# but different amounts
        # (these are separate line items/sub-invoices with same ID)
        invoices = invoices.drop_duplicates(
            subset=['document_number', 'document_date', 'total_amount'],
            keep='first'
        ).reset_index(drop=True)
        
        # Sort invoices by date (for FIFO)
        invoices = invoices.sort_values(['document_date', 'document_number']).copy()
        
        # Group invoices by document_number and aggregate
        # (Same document_number with different amounts = one logical invoice split into parts)
        invoice_groups = invoices.groupby('document_number').agg({
            'document_date': 'first',  # Use first date
            'total_amount': 'sum',      # Sum all amounts for same doc#
            'contract_number': 'first'   # Use first contract
        }).reset_index()

        invoice_groups = invoice_groups.sort_values('document_date').copy()
        invoice_groups['remaining_amount'] = invoice_groups['total_amount'].copy()

        # Also keep original invoices for component generation (to show sub-invoice detail)
        invoices['remaining_amount'] = invoices['total_amount'].copy()
        
        # Remove duplicate payments
        if not payments.empty:
            payments = payments.drop_duplicates(
                subset=['transaction_date', 'amount'],
                keep='first'
            ).reset_index(drop=True)
            payments = payments.sort_values('transaction_date').copy()
            payments['available_amount'] = payments['amount'].copy()
            
            # Parse contract numbers from payment_purpose
            payments['parsed_contract'] = payments.get('payment_purpose', pd.Series()).apply(
                parse_contract_number_from_payment
            )
        
        # Remove duplicate returns
        if not returns.empty:
            returns = returns.drop_duplicates(
                subset=['document_number', 'document_date', 'total_amount'],
                keep='first'
            ).reset_index(drop=True)
        
        # Create a mapping of returns to original invoices
        # GROUP returns by (original_id, date, amount) to avoid duplicate components
        return_map = {}

        # DEBUG: Track invoice #5 specifically
        debug_invoice_5_returns = []

        for _, ret in returns.iterrows():
            original_id = parse_return_invoice_id(ret.get('document_number', ''))
            if original_id:
                ret_amount = ret.get('total_amount', 0)

                # Handle negative return amounts (credit notes stored as negative)
                ret_amount = abs(ret_amount)

                if ret_amount <= 0:
                    continue

                ret_date = ret.get('document_date')
                ret_doc = ret.get('document_number')

                # DEBUG: Capture all returns for invoice #5
                if str(original_id) == '5':
                    debug_invoice_5_returns.append({
                        'return_doc_number': ret_doc,
                        'return_date': ret_date,
                        'return_amount': ret_amount,
                        'original_invoice_id': original_id
                    })

                # Check if this exact return is already in the map to avoid duplicates
                # Match by (date, amount) instead of just document_number
                # to catch duplicate returns with different doc numbers
                ret_data = {
                    'amount': ret_amount,
                    'date': ret_date,
                    'document_number': ret_doc
                }

                if original_id not in return_map:
                    return_map[original_id] = []

                # Only add if not already in the list (check by date AND amount)
                # This catches duplicates even if document_number differs
                if not any(
                    r['date'] == ret_date and abs(r['amount'] - ret_amount) < 1.0
                    for r in return_map[original_id]
                ):
                    return_map[original_id].append(ret_data)

        # PHASE 1: Contract-based payment allocation
        # Use grouped invoices to prevent over-allocation
        if not payments.empty:
            for pay_idx, payment in payments.iterrows():
                parsed_contract = payment.get('parsed_contract')
                available_amt = payment.get('available_amount', 0)
                
                if not parsed_contract or pd.isna(parsed_contract) or available_amt <= 0:
                    continue
                
                pay_date = payment.get('transaction_date')
                if pd.isna(pay_date):
                    continue
                pay_date = pay_date.date() if isinstance(pay_date, pd.Timestamp) else pay_date
                
                # Find invoice GROUPS with matching contract_number
                for grp_idx, invoice_group in invoice_groups.iterrows():
                    invoice_contract = str(invoice_group.get('contract_number', '')).strip()
                    remaining_inv = invoice_group.get('remaining_amount', 0)
                    
                    if not invoice_contract or remaining_inv <= 0:
                        continue
                    
                    # Check if contract numbers match (case-insensitive, stripped)
                    if invoice_contract.upper() == str(parsed_contract).upper():
                        doc_num = invoice_group.get('document_number', '')
                        invoice_date = invoice_group.get('document_date')
                        
                        if pd.isna(invoice_date):
                            continue
                        
                        invoice_date = invoice_date.date() if isinstance(invoice_date, pd.Timestamp) else invoice_date
                        
                        # Get due days from contract-specific terms or default
                        contract_terms = self.config.get('contract_payment_terms', {})
                        due_days = contract_terms.get(invoice_contract, self.config.get('default_due_days', 0))
                        due_date = invoice_date + timedelta(days=due_days)
                        
                        # Allocate payment to this invoice group
                        allocated = min(available_amt, remaining_inv)
                        
                        if allocated > 0:
                            dpd = calculate_dpd(due_date, pay_date)
                            
                            components.append({
                                'invoice_id': doc_num,
                                'invoice_number': doc_num,
                                'counterparty_inn': counterparty_inn,
                                'invoice_date': invoice_date,
                                'due_date': due_date,
                                'component_type': 'paid',
                                'component_amount': float(allocated),
                                'resolution_date': pay_date,
                                'dpd': dpd,
                                'aging_bucket': assign_aging_bucket(dpd, self.config),
                                'payment_method': 'contract_match',
                                'contract_number': invoice_contract,
                                'payment_purpose': str(payment.get('payment_purpose', ''))[:200]  # Truncate for display
                            })
                            
                            # Update remaining amounts in GROUP
                            payments.at[pay_idx, 'available_amount'] -= allocated
                            invoice_groups.at[grp_idx, 'remaining_amount'] -= allocated
                            available_amt -= allocated
                            
                            if available_amt <= 0:
                                break
        
        # PHASE 2: FIFO allocation for remaining payments
        
        # PHASE 2 continued: FIFO allocation for remaining unallocated payments
        # Use grouped invoices to prevent over-allocation
        if not payments.empty:
            for pay_idx, payment in payments.iterrows():
                available_amt = payment.get('available_amount', 0)
                
                if available_amt <= 0:
                    continue
                
                pay_date = payment.get('transaction_date')
                if pd.isna(pay_date):
                    continue
                    
                pay_date = pay_date.date() if isinstance(pay_date, pd.Timestamp) else pay_date
                
                # Allocate to invoice GROUPS in FIFO order (already sorted by date)
                for grp_idx, invoice_group in invoice_groups.iterrows():
                    remaining_inv = invoice_group.get('remaining_amount', 0)
                    
                    if remaining_inv <= 0 or available_amt <= 0:
                        continue
                    
                    doc_num = invoice_group.get('document_number', '')
                    invoice_date = invoice_group.get('document_date')
                    
                    if pd.isna(invoice_date):
                        continue
                    
                    invoice_date = invoice_date.date() if isinstance(invoice_date, pd.Timestamp) else invoice_date
                    
                    # Only allocate payments that came after invoice date
                    if pay_date < invoice_date:
                        continue
                    
                    # Get due days from contract-specific terms or default
                    invoice_contract = str(invoice_group.get('contract_number', '')).strip()
                    contract_terms = self.config.get('contract_payment_terms', {})
                    due_days = contract_terms.get(invoice_contract, self.config.get('default_due_days', 0))
                    due_date = invoice_date + timedelta(days=due_days)
                    
                    # Allocate payment
                    allocated = min(available_amt, remaining_inv)
                    
                    if allocated > 0:
                        dpd = calculate_dpd(due_date, pay_date)
                        
                        components.append({
                            'invoice_id': doc_num,
                            'invoice_number': doc_num,
                            'counterparty_inn': counterparty_inn,
                            'invoice_date': invoice_date,
                            'due_date': due_date,
                            'component_type': 'paid',
                            'component_amount': float(allocated),
                            'resolution_date': pay_date,
                            'dpd': dpd,
                            'aging_bucket': assign_aging_bucket(dpd, self.config),
                            'payment_method': 'fifo',
                            'contract_number': invoice_contract,
                            'payment_purpose': str(payment.get('payment_purpose', ''))[:200]  # Truncate for display
                        })
                        
                        # Update remaining amounts in GROUP
                        payments.at[pay_idx, 'available_amount'] -= allocated
                        invoice_groups.at[grp_idx, 'remaining_amount'] -= allocated
                        available_amt -= allocated
        
        # PHASE 3: Process returns and unpaid balances using invoice GROUPS
        for _, invoice_group in invoice_groups.iterrows():
            doc_num = invoice_group.get('document_number', '')
            invoice_date = invoice_group.get('document_date')
            remaining_amount = invoice_group.get('remaining_amount', 0)
            
            if pd.isna(invoice_date):
                continue
            
            invoice_date = invoice_date.date() if isinstance(invoice_date, pd.Timestamp) else invoice_date
            
            # Get due days from contract-specific terms or default
            invoice_contract = str(invoice_group.get('contract_number', '')).strip()
            contract_terms = self.config.get('contract_payment_terms', {})
            due_days = contract_terms.get(invoice_contract, self.config.get('default_due_days', 0))
            due_date = invoice_date + timedelta(days=due_days)
            
            # Check for returns linked to this document_number
            invoice_returns = return_map.get(str(doc_num), [])
            
            # Process returns for this invoice group
            for ret in invoice_returns:
                ret_amount = ret['amount']
                ret_date = ret['date']
                
                if pd.isna(ret_date) or ret_amount <= 0:
                    continue
                
                ret_date = ret_date.date() if isinstance(ret_date, pd.Timestamp) else ret_date
                
                # Return component
                dpd_return = calculate_dpd(due_date, ret_date)
                
                components.append({
                    'invoice_id': doc_num,
                    'invoice_number': doc_num,
                    'counterparty_inn': counterparty_inn,
                    'invoice_date': invoice_date,
                    'due_date': due_date,
                    'component_type': 'returned',
                    'component_amount': float(ret_amount),
                    'resolution_date': ret_date,
                    'dpd': dpd_return,
                    'aging_bucket': assign_aging_bucket(dpd_return, self.config),
                    'contract_number': invoice_contract,
                    'return_document': ret.get('document_number')
                })
                
                # Reduce remaining amount by return amount
                remaining_amount -= ret_amount
            
            # Any remaining unpaid balance becomes 'open' component
            # (Returns are already handled above at the document_number level)
            if remaining_amount > 0.01:  # Small threshold to avoid rounding issues
                dpd_open = calculate_dpd(due_date, as_of_date)

                components.append({
                    'invoice_id': doc_num,
                    'invoice_number': doc_num,
                    'counterparty_inn': counterparty_inn,
                    'invoice_date': invoice_date,
                    'due_date': due_date,
                    'component_type': 'open',
                    'component_amount': float(remaining_amount),
                    'resolution_date': as_of_date,
                    'dpd': dpd_open,
                    'aging_bucket': assign_aging_bucket(dpd_open, self.config),
                    'contract_number': invoice_contract
                })

        # PHASE 4: Process orphaned returns (returns without corresponding regular invoice in dataset)
        # These are returns where the original invoice is not in invoice_groups (e.g., from a different period)
        processed_invoice_ids = set(str(doc_num) for doc_num in invoice_groups['document_number'].values) if not invoice_groups.empty else set()

        for original_id, return_list in return_map.items():
            # Skip if this invoice was already processed in PHASE 3
            if original_id in processed_invoice_ids:
                continue

            # This is an orphaned return - create components for it
            for ret in return_list:
                ret_amount = ret['amount']
                ret_date = ret['date']
                ret_doc = ret.get('document_number')

                if pd.isna(ret_date) or ret_amount <= 0:
                    continue

                ret_date = ret_date.date() if isinstance(ret_date, pd.Timestamp) else ret_date

                # For orphaned returns, we don't have the original invoice date
                # So we use the return date as a proxy for invoice date
                invoice_date = ret_date

                # Use default payment terms since we don't have the contract info
                due_days = self.config.get('default_due_days', 0)
                due_date = invoice_date + timedelta(days=due_days)

                # Calculate DPD (will be 0 since invoice_date = ret_date)
                dpd_return = calculate_dpd(due_date, ret_date)

                components.append({
                    'invoice_id': original_id,
                    'invoice_number': original_id,
                    'counterparty_inn': counterparty_inn,
                    'invoice_date': invoice_date,
                    'due_date': due_date,
                    'component_type': 'returned',
                    'component_amount': float(ret_amount),
                    'resolution_date': ret_date,
                    'dpd': dpd_return,
                    'aging_bucket': assign_aging_bucket(dpd_return, self.config),
                    'contract_number': None,
                    'return_document': ret_doc,
                    'note': 'Orphaned return (original invoice not in dataset)'
                })

        return components
    
    def calculate_counterparty_risk(
        self,
        counterparty_inn: str,
        components: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """
        Calculate full risk profile for a counterparty.
        
        Args:
            counterparty_inn: Counterparty INN
            components: Optional pre-computed components (uses self.invoice_components if None)
            
        Returns:
            Dictionary with complete risk analysis
        """
        if components is None:
            components = self.invoice_components
        
        # Normalize the counterparty INN for comparison (remove .0 suffix, convert to string)
        normalized_inn = str(counterparty_inn).replace('.0', '').strip()
        
        # Filter components for this counterparty (normalize component INNs too)
        cp_components = []
        unique_inns = set()
        for c in components:
            comp_inn = str(c.get('counterparty_inn', '')).replace('.0', '').strip()
            unique_inns.add(comp_inn)
            if comp_inn == normalized_inn:
                cp_components.append(c)
        
        if not cp_components:
            # Log warning if INN not found (but don't spam console)
            if len(unique_inns) < 50:  # Only log if reasonable number of INNs
                print(f"⚠️  No components found for INN '{normalized_inn}'. Available INNs: {sorted(list(unique_inns))[:10]}...")
            return self._empty_risk_profile(counterparty_inn)
        
        # Calculate behavioral features
        feature_engine = BehavioralFeatureEngine(self.config)
        features = feature_engine.calculate_all_features(cp_components)
        
        # Calculate PD
        pd_model = PDScoringModel(self.config)
        pd_result = pd_model.calculate_pd(features)
        
        # Calculate LGD
        lgd_engine = LGDEngine(self.config)
        lgd_result = lgd_engine.calculate_lgd(features)
        
        # Calculate EAD
        ead_engine = EADEngine(self.config)
        ead_result = ead_engine.calculate_ead(cp_components)
        
        # Calculate Expected Loss
        expected_loss = pd_result['pd'] * lgd_result['lgd'] * ead_result['ead_current']
        
        # Calculate Credit Limit
        limit_engine = CreditLimitEngine(self.config)
        limit_result = limit_engine.calculate_recommended_limit(
            pd_result, lgd_result, ead_result, features
        )
        
        # Compile full risk profile
        risk_profile = {
            'counterparty_inn': counterparty_inn,
            'analysis_date': date.today().isoformat(),
            'rating': pd_result['rating'],
            'pd': pd_result['pd'],
            'pd_score': pd_result['score'],
            'lgd': lgd_result['lgd'],
            'ead_current': ead_result['ead_current'],
            'ead_peak_3m': ead_result['ead_peak_3m'],
            'ead_peak_12m': ead_result['ead_peak_12m'],
            'expected_loss': expected_loss,
            'recommended_limit': limit_result['recommended_limit'],
            'limit_method': limit_result['method'],
            'behavioral_features': features,
            'component_count': len(cp_components),
            'justification': self._build_justification(pd_result, lgd_result, ead_result, features)
        }
        
        return decimal_to_float(risk_profile)
    
    def _empty_risk_profile(self, counterparty_inn: str) -> Dict[str, Any]:
        """Return empty risk profile for counterparty with no data."""
        return {
            'counterparty_inn': counterparty_inn,
            'analysis_date': date.today().isoformat(),
            'rating': 'N/A',
            'pd': 0.0,
            'lgd': 0.0,
            'ead_current': 0.0,
            'expected_loss': 0.0,
            'recommended_limit': 0.0,
            'error': 'No invoice data available for this counterparty'
        }
    
    def _build_justification(
        self,
        pd_result: Dict,
        lgd_result: Dict,
        ead_result: Dict,
        features: Dict
    ) -> Dict[str, str]:
        """Build human-readable justification for risk assessment."""
        justification = {}
        
        # PD justification
        pd_factors = []
        if features.get('max_dpd_paid', 0) > 60:
            pd_factors.append(f"Maximum DPD of {features['max_dpd_paid']:.0f} days exceeds 60 days")
        if features.get('share_exposure_gt90', 0) > 0.30:
            pd_factors.append(f"{features['share_exposure_gt90']:.1%} of exposure is >90 DPD")
        if features.get('weighted_avg_dpd', 0) > 30:
            pd_factors.append(f"Weighted average DPD is {features['weighted_avg_dpd']:.0f} days")
        if features.get('return_ratio', 0) > 0.05:
            pd_factors.append(f"Return ratio of {features['return_ratio']:.1%} exceeds 5%")
        
        justification['pd'] = '; '.join(pd_factors) if pd_factors else 'Good payment behavior'
        
        # LGD justification
        lgd_factors = []
        if features.get('share_exposure_gt180', 0) > 0.20:
            lgd_factors.append(f"{features['share_exposure_gt180']:.1%} of exposure is >180 DPD")
        if features.get('return_ratio', 0) > 0.05:
            lgd_factors.append(f"High dilution risk with {features['return_ratio']:.1%} returns")
        
        justification['lgd'] = '; '.join(lgd_factors) if lgd_factors else 'Standard recourse factoring'
        
        # EAD justification
        justification['ead'] = f"Current exposure: {ead_result['ead_current']:,.2f}"
        
        return justification


class BehavioralFeatureEngine:
    """Calculate behavioral features from invoice components."""
    
    def __init__(self, config: Dict):
        self.config = config
    
    def calculate_all_features(self, components: List[Dict]) -> Dict[str, float]:
        """Calculate all behavioral features."""
        if not components:
            return self._empty_features()
        
        features = {}
        
        # Exposure features
        features.update(self._calculate_exposure_features(components))
        
        # Delinquency features
        features.update(self._calculate_delinquency_features(components))
        
        # Credit quality indicators
        features.update(self._calculate_credit_quality(components))
        
        return features
    
    def _calculate_exposure_features(self, components: List[Dict]) -> Dict[str, float]:
        """Calculate exposure-related features."""
        df = pd.DataFrame(components)
        
        # Total amounts by component type
        total_invoiced = df['component_amount'].sum()
        total_paid = df[df['component_type'] == 'paid']['component_amount'].sum()
        total_returns = df[df['component_type'] == 'returned']['component_amount'].sum()
        total_unpaid = df[df['component_type'] == 'open']['component_amount'].sum()
        
        # Monthly volatility (if we have date information)
        exposure_volatility = 0.0
        if 'invoice_date' in df.columns:
            df['invoice_month'] = pd.to_datetime(df['invoice_date']).dt.to_period('M')
            monthly_amounts = df.groupby('invoice_month')['component_amount'].sum()
            if len(monthly_amounts) > 1:
                exposure_volatility = monthly_amounts.std()
        
        # Peak exposure
        exposure_peak = df['component_amount'].max()
        
        return {
            'total_invoiced_12m': float(total_invoiced),
            'total_paid_12m': float(total_paid),
            'total_returns_12m': float(total_returns),
            'total_unpaid_current': float(total_unpaid),
            'exposure_volatility': float(exposure_volatility),
            'exposure_peak_12m': float(exposure_peak)
        }
    
    def _calculate_delinquency_features(self, components: List[Dict]) -> Dict[str, float]:
        """Calculate delinquency-related features."""
        df = pd.DataFrame(components)
        
        # Max DPD among paid components
        paid_components = df[df['component_type'] == 'paid']
        max_dpd_paid = paid_components['dpd'].max() if not paid_components.empty else 0
        
        # Weighted average DPD
        if df['component_amount'].sum() > 0:
            weighted_avg_dpd = (df['dpd'] * df['component_amount']).sum() / df['component_amount'].sum()
        else:
            weighted_avg_dpd = 0
        
        # Share of exposure by DPD thresholds
        total_exposure = df['component_amount'].sum()
        if total_exposure > 0:
            share_gt30 = df[df['dpd'] > 30]['component_amount'].sum() / total_exposure
            share_gt60 = df[df['dpd'] > 60]['component_amount'].sum() / total_exposure
            share_gt90 = df[df['dpd'] > 90]['component_amount'].sum() / total_exposure
            share_gt180 = df[df['dpd'] > 180]['component_amount'].sum() / total_exposure
        else:
            share_gt30 = share_gt60 = share_gt90 = share_gt180 = 0
        
        # Count metrics
        count_with_returns = df[df['component_type'] == 'returned']['invoice_id'].nunique()
        count_unpaid_gt90 = df[(df['component_type'] == 'open') & (df['dpd'] > 90)]['invoice_id'].nunique()
        
        return {
            'max_dpd_paid': float(max_dpd_paid),
            'weighted_avg_dpd': float(weighted_avg_dpd),
            'share_exposure_gt30': float(share_gt30),
            'share_exposure_gt60': float(share_gt60),
            'share_exposure_gt90': float(share_gt90),
            'share_exposure_gt180': float(share_gt180),
            'count_invoices_with_returns': int(count_with_returns),
            'count_invoices_unpaid_gt90': int(count_unpaid_gt90)
        }
    
    def _calculate_credit_quality(self, components: List[Dict]) -> Dict[str, float]:
        """Calculate credit quality indicators."""
        df = pd.DataFrame(components)
        
        # Late payment frequency (payments with DPD > 0)
        paid_components = df[df['component_type'] == 'paid']
        if not paid_components.empty:
            late_payments = (paid_components['dpd'] > 0).sum()
            total_payments = len(paid_components)
            late_payment_rate = late_payments / total_payments if total_payments > 0 else 0
        else:
            late_payment_rate = 0
        
        # Payment regularity (coefficient of variation of DPD for paid invoices)
        payment_regularity_score = 0.0
        if not paid_components.empty and len(paid_components) > 1:
            dpd_values = paid_components['dpd'].tolist()
            payment_regularity_score = calculate_coefficient_of_variation(dpd_values)
        
        # Dispute frequency (number of returns)
        dispute_frequency = len(df[df['component_type'] == 'returned'])
        
        # Return ratio
        total_invoiced = df['component_amount'].sum()
        total_returns = df[df['component_type'] == 'returned']['component_amount'].sum()
        return_ratio = total_returns / total_invoiced if total_invoiced > 0 else 0
        
        return {
            'late_payment_rate': float(late_payment_rate),
            'payment_regularity_score': float(payment_regularity_score),
            'dispute_frequency': int(dispute_frequency),
            'return_ratio': float(return_ratio)
        }
    
    def _empty_features(self) -> Dict[str, float]:
        """Return empty features dict."""
        return {
            'total_invoiced_12m': 0.0,
            'total_paid_12m': 0.0,
            'total_returns_12m': 0.0,
            'total_unpaid_current': 0.0,
            'exposure_volatility': 0.0,
            'exposure_peak_12m': 0.0,
            'max_dpd_paid': 0.0,
            'weighted_avg_dpd': 0.0,
            'share_exposure_gt30': 0.0,
            'share_exposure_gt60': 0.0,
            'share_exposure_gt90': 0.0,
            'share_exposure_gt180': 0.0,
            'count_invoices_with_returns': 0,
            'count_invoices_unpaid_gt90': 0,
            'late_payment_rate': 0.0,
            'payment_regularity_score': 0.0,
            'dispute_frequency': 0,
            'return_ratio': 0.0
        }


class PDScoringModel:
    """PD (Probability of Default) scoring model."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.weights = config.get('pd_weights', {}).get('default', [2.0, 2.0, 3.0, 1.0, 1.0])
        
    def calculate_pd(self, features: Dict[str, float]) -> Dict[str, Any]:
        """
        Calculate PD score, rating, and probability.
        
        Args:
            features: Behavioral features dictionary
            
        Returns:
            Dict with score, rating, and pd
        """
        # Calculate rule-based score
        score = self._calculate_score(features)
        
        # Assign rating
        rating = self._assign_rating(score)
        
        # Get PD range
        pd = self._get_pd_probability(rating)
        
        return {
            'score': score,
            'rating': rating,
            'pd': pd,
            'score_breakdown': self._get_score_breakdown(features)
        }
    
    def _calculate_score(self, features: Dict[str, float]) -> float:
        """Calculate PD score using weighted indicators."""
        w1, w2, w3, w4, w5 = self.weights
        
        # Indicator functions
        ind_max_dpd_gt60 = 1.0 if features.get('max_dpd_paid', 0) > 60 else 0.0
        ind_share_exp_gt90 = 1.0 if features.get('share_exposure_gt90', 0) > 0.30 else 0.0
        ind_any_dpd_gt180 = 1.0 if features.get('share_exposure_gt180', 0) > 0 else 0.0
        
        # Continuous components
        weighted_avg_dpd_component = features.get('weighted_avg_dpd', 0) / 30.0
        return_ratio_component = features.get('return_ratio', 0)
        
        score = (
            w1 * ind_max_dpd_gt60 +
            w2 * ind_share_exp_gt90 +
            w3 * ind_any_dpd_gt180 +
            w4 * weighted_avg_dpd_component +
            w5 * return_ratio_component * 100  # Scale return ratio
        )
        
        return score
    
    def _assign_rating(self, score: float) -> str:
        """Assign rating based on score thresholds."""
        thresholds = self.config.get('rating_thresholds', {
            'A': [0, 2],
            'B': [3, 4],
            'C': [5, 6],
            'D': [7, 100]
        })
        
        for rating, (min_score, max_score) in thresholds.items():
            if min_score <= score <= max_score:
                return rating
        
        return 'D'  # Default to worst rating
    
    def _get_pd_probability(self, rating: str) -> float:
        """Get PD probability from rating (midpoint of range)."""
        pd_ranges = self.config.get('pd_ranges', {
            'A': [0.01, 0.02],
            'B': [0.03, 0.05],
            'C': [0.06, 0.10],
            'D': [0.10, 0.20]
        })
        
        pd_range = pd_ranges.get(rating, [0.10, 0.20])
        return sum(pd_range) / 2  # Midpoint
    
    def _get_score_breakdown(self, features: Dict[str, float]) -> Dict[str, float]:
        """Get detailed breakdown of score components."""
        w1, w2, w3, w4, w5 = self.weights
        
        breakdown = {
            'max_dpd_indicator': w1 if features.get('max_dpd_paid', 0) > 60 else 0.0,
            'high_delinquency_indicator': w2 if features.get('share_exposure_gt90', 0) > 0.30 else 0.0,
            'severe_delinquency_indicator': w3 if features.get('share_exposure_gt180', 0) > 0 else 0.0,
            'weighted_avg_dpd_component': w4 * features.get('weighted_avg_dpd', 0) / 30.0,
            'return_ratio_component': w5 * features.get('return_ratio', 0) * 100
        }
        
        return breakdown


class LGDEngine:
    """LGD (Loss Given Default) calculation engine."""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def calculate_lgd(self, features: Dict[str, float]) -> Dict[str, Any]:
        """
        Calculate LGD based on delinquency and dilution patterns.
        
        Args:
            features: Behavioral features dictionary
            
        Returns:
            Dict with lgd and breakdown
        """
        lgd_config = self.config.get('lgd_config', {})
        base_lgd = lgd_config.get('base_lgd', 0.50)
        adjustments = lgd_config.get('adjustments', {})
        max_lgd = lgd_config.get('max_lgd', 0.90)
        
        lgd = base_lgd
        lgd_adjustments = {}
        
        # Adjust for severe delinquency
        severe_delinq_threshold = adjustments.get('severe_delinquency_threshold', 0.20)
        severe_delinq_adj = adjustments.get('severe_delinquency_adjustment', 0.10)
        
        if features.get('share_exposure_gt180', 0) > severe_delinq_threshold:
            lgd += severe_delinq_adj
            lgd_adjustments['severe_delinquency'] = severe_delinq_adj
        
        # Adjust for high return rate (dilution risk)
        high_return_threshold = adjustments.get('high_return_threshold', 0.05)
        high_return_adj = adjustments.get('high_return_adjustment', 0.05)
        
        if features.get('return_ratio', 0) > high_return_threshold:
            lgd += high_return_adj
            lgd_adjustments['dilution_risk'] = high_return_adj
        
        # Cap at max LGD
        lgd = min(lgd, max_lgd)
        
        return {
            'lgd': lgd,
            'base_lgd': base_lgd,
            'adjustments': lgd_adjustments,
            'final_lgd': lgd
        }


class EADEngine:
    """EAD (Exposure At Default) calculation engine."""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def calculate_ead(self, components: List[Dict]) -> Dict[str, float]:
        """
        Calculate current, peak, and volatility of EAD.
        
        Args:
            components: List of invoice components
            
        Returns:
            Dict with EAD metrics
        """
        if not components:
            return {
                'ead_current': 0.0,
                'ead_peak_3m': 0.0,
                'ead_peak_12m': 0.0,
                'ead_volatility': 0.0
            }
        
        df = pd.DataFrame(components)
        
        # Current EAD = sum of open components
        ead_current = df[df['component_type'] == 'open']['component_amount'].sum()
        
        # Peak EAD calculations (simplified - using max invoice amount as proxy)
        ead_peak_12m = df['component_amount'].max()
        ead_peak_3m = ead_peak_12m  # Simplified
        
        # EAD volatility
        ead_volatility = df['component_amount'].std() if len(df) > 1 else 0.0
        
        return {
            'ead_current': float(ead_current),
            'ead_peak_3m': float(ead_peak_3m),
            'ead_peak_12m': float(ead_peak_12m),
            'ead_volatility': float(ead_volatility)
        }


class CreditLimitEngine:
    """Credit limit recommendation engine."""
    
    def __init__(self, config: Dict):
        self.config = config
        
    def calculate_recommended_limit(
        self,
        pd_result: Dict,
        lgd_result: Dict,
        ead_result: Dict,
        features: Dict
    ) -> Dict[str, Any]:
        """
        Calculate recommended credit limit using dual methods.
        
        Args:
            pd_result: PD calculation results
            lgd_result: LGD calculation results
            ead_result: EAD calculation results
            features: Behavioral features
            
        Returns:
            Dict with recommended limit and method
        """
        # Method 1: Risk-based formula
        risk_cap = self.config.get('risk_cap_default', 1000000)
        pd = pd_result['pd']
        lgd = lgd_result['lgd']
        
        if pd > 0 and lgd > 0:
            risk_based_limit = risk_cap / (pd * lgd)
        else:
            risk_based_limit = 0
        
        # Method 2: Rating-based multiplier
        monthly_volume = features.get('total_invoiced_12m', 0) / 12
        rating = pd_result['rating']
        multipliers = self.config.get('limit_multipliers', {
            'A': 2.0, 'B': 1.0, 'C': 0.7, 'D': 0.3
        })
        multiplier = multipliers.get(rating, 0.5)
        rating_based_limit = monthly_volume * multiplier
        
        # Take conservative approach (minimum of both)
        recommended_limit = min(risk_based_limit, rating_based_limit) if risk_based_limit > 0 else rating_based_limit
        
        return {
            'recommended_limit': float(recommended_limit),
            'risk_based_limit': float(risk_based_limit),
            'rating_based_limit': float(rating_based_limit),
            'method': 'conservative_minimum',
            'monthly_volume': float(monthly_volume),
            'multiplier': multiplier
        }

