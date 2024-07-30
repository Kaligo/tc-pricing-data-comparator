import os
from datetime import datetime
from compare_pricing import load_data, compare_pricing

def main():
    old_file = 'data/2022_old_pricing_query.csv'
    new_file = 'data/2022_new_pricing_query.csv'
    
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
