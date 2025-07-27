import pandas as pd
import numpy as np

def fill_null_values(df):
    """
    Fill null values in the dataframe with appropriate strategies:
    - Numeric columns: Fill with median
    - Categorical columns: Fill with mode
    - Boolean columns: Fill with False
    """
    print("\n=== NULL VALUE IMPUTATION ===\n")
    
    # Track original null counts
    null_counts = df.isnull().sum()
    if null_counts.sum() == 0:
        print("No null values found in the dataset.")
        return df
    
    print("Null values before imputation:")
    print(null_counts[null_counts > 0])
    
    # Fill numerical columns with median
    num_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in num_cols:
        if df[col].isnull().sum() > 0:
            df[col].fillna(df[col].median(), inplace=True)
    
    # Fill categorical columns with mode
    cat_cols = df.select_dtypes(include=['object']).columns
    for col in cat_cols:
        if df[col].isnull().sum() > 0:
            df[col].fillna(df[col].mode()[0], inplace=True)
    
    # Fill boolean columns with False
    bool_cols = df.select_dtypes(include=['bool']).columns
    for col in bool_cols:
        if df[col].isnull().sum() > 0:
            df[col].fillna(False, inplace=True)
    
    print("\nNull values after imputation:")
    print(df.isnull().sum()[df.isnull().sum() > 0])
    
    return df

def main():
    # File paths
    input_file = 'dataset.csv'  # Update with your file path
    output_file = 'filled_transactions.csv'
    
    try:
        # Load data
        print(f"Loading data from {input_file}...")
        df = pd.read_csv(input_file)
        print(f"Data loaded successfully. Shape: {df.shape}")
        
        # Fill null values
        filled_df = fill_null_values(df)
        
        # Save processed data
        filled_df.to_csv(output_file, index=False)
        print(f"\nData with filled null values saved to {output_file}")
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    df = pd.read_csv('dataset.csv')
    has_nulls = df.isnull().values.any()
    print("Contains null values:", has_nulls)