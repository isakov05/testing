"""
Risk Engine Utility Functions

Helper functions for the factoring risk engine including:
- Return invoice ID parsing
- DPD calculations
- Aging bucket assignments
- Report formatting
- Excel export
"""

import re
import json
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List
import pandas as pd


def parse_contract_number_from_payment(payment_purpose: str) -> Optional[str]:
    """
    Extract contract number from payment purpose text.
    
    Examples:
        "00517 оплата 100 % По Договор № PБПRV/1 от 04.01.2025 за Лекарственные средства"
        → Returns: "PБПRV/1"
        
        "Оплата по Договор № ABC-123 от 15.02.2025"
        → Returns: "ABC-123"
    
    Args:
        payment_purpose: Payment purpose text
        
    Returns:
        Contract number if found, None otherwise
    """
    if not payment_purpose:
        return None
    
    # Pattern: Find text after "Договор №" or "Договор№" until "от" or end of meaningful text
    # Handles variations: "Договор №", "Договор№", "договор №", etc.
    patterns = [
        r'[Дд]оговор\s*№\s*([^\s]+(?:\s+[^\s]+)?)\s+от',  # "Договор № XXX от"
        r'[Дд]оговор\s*№\s*([A-Za-zА-Яа-я0-9/_-]+)',      # "Договор № XXX" (contract code)
        r'[Cc]ontract\s*[#№]\s*([A-Za-z0-9/_-]+)',        # English "Contract #"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, str(payment_purpose), re.IGNORECASE)
        if match:
            contract_num = match.group(1).strip()
            # Remove trailing punctuation
            contract_num = re.sub(r'[,.\s]+$', '', contract_num)
            return contract_num
    
    return None


def parse_return_invoice_id(document_number: str) -> Optional[str]:
    """
    Extract original invoice ID from return document number.
    
    Examples:
        '5/возврат' -> '5'
        '123/return' -> '123'
        'INV-456/возврат' -> 'INV-456'
        '789/RETURN' -> '789'
        'возврат/5' -> None (invalid format)
    
    Args:
        document_number: The document number to parse
        
    Returns:
        Original invoice ID if pattern matches, None otherwise
    """
    if not document_number:
        return None
    
    # Pattern: capture everything before the first "/" followed by return/возврат
    pattern = r'^([^/]+)/.*(?:возврат|return)'
    match = re.match(pattern, str(document_number), re.IGNORECASE)
    
    if match:
        return match.group(1).strip()
    
    return None


def is_return_document(document_number: str) -> bool:
    """
    Check if a document number indicates a return/credit note.
    
    Args:
        document_number: The document number to check
        
    Returns:
        True if document is a return, False otherwise
    """
    if not document_number:
        return False
    
    doc_str = str(document_number).lower()
    return 'возврат' in doc_str or 'return' in doc_str


def calculate_dpd(due_date: date, resolution_date: date) -> int:
    """
    Calculate Days Past Due (DPD).
    
    Args:
        due_date: The invoice due date
        resolution_date: The payment/resolution date
        
    Returns:
        Number of days past due (can be negative if paid early)
    """
    if not due_date or not resolution_date:
        return 0
    
    if isinstance(due_date, datetime):
        due_date = due_date.date()
    if isinstance(resolution_date, datetime):
        resolution_date = resolution_date.date()
    
    return (resolution_date - due_date).days


def assign_aging_bucket(dpd: int, config: Optional[Dict] = None) -> str:
    """
    Assign aging bucket based on DPD.
    
    Args:
        dpd: Days past due
        config: Optional config dict with aging_buckets
        
    Returns:
        Aging bucket name (e.g., '0-30', '31-60', '180+')
    """
    if config and 'aging_buckets' in config:
        buckets = config['aging_buckets']
    else:
        # Default buckets
        buckets = [
            {"name": "0-30", "min": 0, "max": 30},
            {"name": "31-60", "min": 31, "max": 60},
            {"name": "61-90", "min": 61, "max": 90},
            {"name": "91-180", "min": 91, "max": 180},
            {"name": "180+", "min": 181, "max": 999999}
        ]
    
    for bucket in buckets:
        if bucket['min'] <= dpd <= bucket['max']:
            return bucket['name']
    
    # Default fallback
    if dpd < 0:
        return "Prepaid"
    return "180+"


def load_risk_config(config_path: str = "config/risk_config.json") -> Dict[str, Any]:
    """
    Load risk engine configuration from JSON file.
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config from {config_path}: {e}")
        # Return minimal default config
        return {
            "pd_weights": {"default": [2.0, 2.0, 3.0, 1.0, 1.0]},
            "default_due_days": 30,
            "aging_buckets": [
                {"name": "0-30", "min": 0, "max": 30},
                {"name": "31-60", "min": 31, "max": 60},
                {"name": "61-90", "min": 61, "max": 90},
                {"name": "91-180", "min": 91, "max": 180},
                {"name": "180+", "min": 181, "max": 999999}
            ]
        }


def save_risk_config(config: Dict[str, Any], config_path: str = "config/risk_config.json") -> bool:
    """
    Save risk engine configuration to JSON file.
    
    Args:
        config: Configuration dictionary
        config_path: Path to save config file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving config to {config_path}: {e}")
        return False


def format_risk_report(counterparty_data: Dict[str, Any], format_type: str = "json") -> str:
    """
    Format risk analysis results for output.
    
    Args:
        counterparty_data: Risk analysis results dictionary
        format_type: Output format ('json', 'text', 'markdown')
        
    Returns:
        Formatted report string
    """
    if format_type == "json":
        return json.dumps(counterparty_data, indent=2, default=str, ensure_ascii=False)
    
    elif format_type == "text":
        lines = []
        lines.append("=" * 70)
        lines.append(f"RISK ANALYSIS REPORT - {counterparty_data.get('counterparty_name', 'N/A')}")
        lines.append(f"INN: {counterparty_data.get('counterparty_inn', 'N/A')}")
        lines.append(f"Analysis Date: {counterparty_data.get('analysis_date', 'N/A')}")
        lines.append("=" * 70)
        lines.append("")
        
        lines.append("RISK METRICS:")
        lines.append(f"  Rating:              {counterparty_data.get('rating', 'N/A')}")
        lines.append(f"  PD (Probability):    {counterparty_data.get('pd', 0):.2%}")
        lines.append(f"  LGD (Loss Given):    {counterparty_data.get('lgd', 0):.2%}")
        lines.append(f"  EAD (Exposure):      {counterparty_data.get('ead_current', 0):,.2f}")
        lines.append(f"  Expected Loss:       {counterparty_data.get('expected_loss', 0):,.2f}")
        lines.append("")
        
        lines.append("CREDIT LIMIT:")
        lines.append(f"  Recommended Limit:   {counterparty_data.get('recommended_limit', 0):,.2f}")
        lines.append(f"  Current Exposure:    {counterparty_data.get('ead_current', 0):,.2f}")
        lines.append(f"  Utilization:         {counterparty_data.get('utilization_pct', 0):.1f}%")
        lines.append("")
        
        return "\n".join(lines)
    
    elif format_type == "markdown":
        lines = []
        lines.append(f"# Risk Analysis Report")
        lines.append(f"## {counterparty_data.get('counterparty_name', 'N/A')}")
        lines.append(f"**INN:** {counterparty_data.get('counterparty_inn', 'N/A')}  ")
        lines.append(f"**Analysis Date:** {counterparty_data.get('analysis_date', 'N/A')}")
        lines.append("")
        
        lines.append("## Risk Metrics")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Rating | {counterparty_data.get('rating', 'N/A')} |")
        lines.append(f"| PD | {counterparty_data.get('pd', 0):.2%} |")
        lines.append(f"| LGD | {counterparty_data.get('lgd', 0):.2%} |")
        lines.append(f"| EAD | {counterparty_data.get('ead_current', 0):,.2f} |")
        lines.append(f"| Expected Loss | {counterparty_data.get('expected_loss', 0):,.2f} |")
        lines.append("")
        
        return "\n".join(lines)
    
    return str(counterparty_data)


def export_to_excel(risk_results: List[Dict[str, Any]], output_path: str) -> bool:
    """
    Export risk analysis results to Excel file.
    
    Args:
        risk_results: List of risk analysis result dictionaries
        output_path: Path to output Excel file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Convert to DataFrame
        df = pd.DataFrame(risk_results)
        
        # Create Excel writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Summary sheet
            summary_cols = [
                'counterparty_inn', 'counterparty_name', 'rating', 
                'pd', 'lgd', 'ead_current', 'expected_loss', 
                'recommended_limit'
            ]
            summary_df = df[[col for col in summary_cols if col in df.columns]]
            summary_df.to_excel(writer, sheet_name='Risk Summary', index=False)
            
            # Detailed metrics sheet
            if 'behavioral_features' in df.columns:
                # Expand behavioral features if present
                features_list = []
                for idx, row in df.iterrows():
                    features = row.get('behavioral_features', {})
                    features['counterparty_inn'] = row.get('counterparty_inn')
                    features['counterparty_name'] = row.get('counterparty_name')
                    features_list.append(features)
                
                features_df = pd.DataFrame(features_list)
                features_df.to_excel(writer, sheet_name='Behavioral Features', index=False)
            
            # Full details sheet
            df.to_excel(writer, sheet_name='Full Details', index=False)
        
        return True
    
    except Exception as e:
        print(f"Error exporting to Excel: {e}")
        return False


def decimal_to_float(obj: Any) -> Any:
    """
    Convert Decimal objects to float for JSON serialization.
    
    Args:
        obj: Object to convert
        
    Returns:
        Converted object
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (date, datetime)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(item) for item in obj]
    return obj


def calculate_herfindahl_index(exposures: List[float]) -> float:
    """
    Calculate Herfindahl-Hirschman Index for concentration risk.
    
    Args:
        exposures: List of exposure amounts
        
    Returns:
        HHI index (0 to 1, where 1 = maximum concentration)
    """
    if not exposures or sum(exposures) == 0:
        return 0.0
    
    total = sum(exposures)
    shares = [exp / total for exp in exposures]
    hhi = sum(share ** 2 for share in shares)
    
    return hhi


def calculate_coefficient_of_variation(values: List[float]) -> float:
    """
    Calculate coefficient of variation (std dev / mean).
    
    Args:
        values: List of numeric values
        
    Returns:
        Coefficient of variation
    """
    if not values or len(values) < 2:
        return 0.0
    
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    std_dev = variance ** 0.5
    
    return std_dev / mean

