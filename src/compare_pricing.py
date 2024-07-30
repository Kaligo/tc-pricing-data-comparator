import pandas as pd
import os
from datetime import datetime

def load_data(old_file, new_file):
    old_df = pd.read_csv(old_file)
    new_df = pd.read_csv(new_file)
    return old_df, new_df

def create_key(df):
    return df['tc_customer_id'].astype(str) + '-' + df['tc_loyalty_program_id'].astype(str) + '-' + df['tc_min_created_at'].astype(str)

def percentage_difference(old, new):
    return ((new - old) / old) * 100 if old != 0 else None

def compare_pricing(old_df, new_df):
    old_df['key'] = create_key(old_df)
    new_df['key'] = create_key(new_df)

    merged_df = pd.merge(old_df, new_df, on='key', how='outer', suffixes=('_old', '_new'), indicator=True)

    comparison_columns = ['key', 'tc_customer_id_old', 'tc_loyalty_program_id_old', 'tc_min_created_at_old', 'tc_min_created_at_new', 'tc_max_created_at_new']
    comparison_df = merged_df[comparison_columns].copy()

    #  key exists in both datasets, only in the old dataset, or only in the new dataset
    comparison_df['key_existence'] = merged_df['_merge'].apply(
        lambda x: 'Exists in Both' if x == 'both' else ('Only in Old' if x == 'left_only' else 'Only in New')
    )

    # Comparison for tc_min_created_at
    comparison_df['compare_tc_min_created_at'] = merged_df.apply(
        lambda row: "Matched" if (row['tc_min_created_at_old'] == row['tc_min_created_at_new']) or (pd.isnull(row['tc_min_created_at_old']) and pd.isnull(row['tc_min_created_at_new'])) else (
            "Not Match" if pd.notnull(row['tc_min_created_at_new']) else "Missing in New"), axis=1)

    # Comparison for tc_max_created_at
    comparison_df['compare_tc_max_created_at'] = merged_df.apply(
        lambda row: "Matched" if (row['tc_max_created_at_old'] == row['tc_max_created_at_new']) or (pd.isnull(row['tc_max_created_at_old']) and pd.isnull(row['tc_max_created_at_new'])) else (
            "Not Match" if pd.notnull(row['tc_max_created_at_new']) else "Missing in New"), axis=1)


    comparison_df['selling_price_usd_old'] = merged_df['selling_price_usd_old']
    comparison_df['selling_price_usd_new'] = merged_df['selling_price_usd_new']
    comparison_df['compare_selling_price_usd'] = merged_df.apply(
        lambda row: "Matched" if (row['selling_price_usd_old'] == row['selling_price_usd_new']) or (pd.isnull(row['selling_price_usd_old']) and pd.isnull(row['selling_price_usd_new'])) else (
            "Not Match" if pd.notnull(row['selling_price_usd_new']) else "Missing in New"), axis=1)
    comparison_df['percentage_diff_selling_price_usd'] = merged_df.apply(
        lambda row: percentage_difference(row['selling_price_usd_old'], row['selling_price_usd_new']) if pd.notnull(row['selling_price_usd_new']) and pd.notnull(row['selling_price_usd_old']) else None, axis=1)

    comparison_df['cost_per_point_usd_old'] = merged_df['cost_per_point_usd_old']
    comparison_df['cost_per_point_usd_new'] = merged_df['cost_per_point_usd_new']
    comparison_df['compare_cost_per_point_usd'] = merged_df.apply(
        lambda row: "Matched" if (row['cost_per_point_usd_old'] == row['cost_per_point_usd_new']) or (pd.isnull(row['cost_per_point_usd_old']) and pd.isnull(row['cost_per_point_usd_new'])) else (
            "Not Match" if pd.notnull(row['cost_per_point_usd_new']) else "Missing in New"), axis=1)
    comparison_df['percentage_diff_cost_per_point_usd'] = merged_df.apply(
        lambda row: percentage_difference(row['cost_per_point_usd_old'], row['cost_per_point_usd_new']) if pd.notnull(row['cost_per_point_usd_new']) and pd.notnull(row['cost_per_point_usd_old']) else None, axis=1)

    comparison_df['transfer_ratio_old'] = merged_df['transfer_ratio_old']
    comparison_df['transfer_ratio_new'] = merged_df['transfer_ratio_new']
    comparison_df['compare_transfer_ratio'] = merged_df.apply(
        lambda row: "Matched" if (row['transfer_ratio_old'] == row['transfer_ratio_new']) or (pd.isnull(row['transfer_ratio_old']) and pd.isnull(row['transfer_ratio_new'])) else (
            "Not Match" if pd.notnull(row['transfer_ratio_new']) else "Missing in New"), axis=1)
    comparison_df['percentage_diff_transfer_ratio'] = merged_df.apply(
        lambda row: percentage_difference(row['transfer_ratio_old'], row['transfer_ratio_new']) if pd.notnull(row['transfer_ratio_new']) and pd.notnull(row['transfer_ratio_old']) else None, axis=1)

    comparison_df['start_date_old'] = merged_df['start_date_old']
    comparison_df['start_date_new'] = merged_df['start_date_new']
    comparison_df['compare_start_date'] = merged_df.apply(
        lambda row: "Matched" if (row['start_date_old'] == row['start_date_new']) or (pd.isnull(row['start_date_old']) and pd.isnull(row['start_date_new'])) else (
            "Not Match" if pd.notnull(row['start_date_new']) else "Missing in New"), axis=1)

    return comparison_df

def main():
    old_file = 'data/old_pricing_query_20240730.csv'
    new_file = 'data/new_pricing_query_20240730.csv'
    
    old_df, new_df = load_data(old_file, new_file)
    comparison_df = compare_pricing(old_df, new_df)

    # Ensure the output directory exists
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Get the current date and time for the filename
    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f'comparison_result_{now}.csv')
    
    comparison_df.to_csv(output_file, index=False)
    print(f"Comparison completed. Results saved to '{output_file}'.")

if __name__ == "__main__":
    main()

