import re
import pandas as pd

def clean_rate(rate_text):
    """
    Cleans a string to find a float (e.g., "14.5% p.a." -> 14.5)
    """
    if isinstance(rate_text, str) and rate_text.strip() not in ['-', '–']:
        match = re.search(r'([\d.]+)', rate_text)
        return float(match.group(1)) if match else None
    return None

def parse_term_to_months(term_text):
    """
    Parses a string to find the number of months (e.g., "1 Year" -> 12)
    """
    if not isinstance(term_text, str):
        return None
        
    term_text = term_text.lower()
    
    if 'year' in term_text:
        match = re.search(r'(\d+)', term_text)
        return int(match.group(1)) * 12 if match else None
    elif 'month' in term_text:
        match = re.search(r'(\d+)', term_text)
        return int(match.group(1)) if match else None
    
    return None

def clean_and_rename_df(df):
    """
    Standardizes the DataFrame before it's uploaded to Supabase.
    """
    df_renamed = df.rename(columns={
        'Bank Name': 'bankName',
        'FD Type': 'fdType',
        'Institution Type': 'institutionType',
        'Term (Months)': 'termMonths',
        'Payout Schedule': 'payoutSchedule',
        'Interest Rate (p.a.)': 'interestRate',
        'Annual Effective Rate': 'aer'
    })
    
    # Ensure all required columns exist, add if missing
    required_cols = ['bankName', 'fdType', 'institutionType', 'termMonths', 'payoutSchedule', 'interestRate', 'aer']
    for col in required_cols:
        if col not in df_renamed.columns:
            df_renamed[col] = None
            
    # Select only the required columns and handle NaN/NaT
    df_final = df_renamed[required_cols]
    df_final = df_final.where(pd.notna(df_final), None)
    return df_final.sort_values(by=['bankName', 'termMonths']).reset_index(drop=True)