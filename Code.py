import pandas as pd
from datetime import timedelta, datetime

class StockAnalysis:

    def __init__(self, daily_data_path, intraday_data_19_path, intraday_data_22_path):
        """
        Initialize the StockAnalysis object by loading the required data.
        """
        self.daily_data = pd.read_csv(daily_data_path)
        self.intraday_data_19 = pd.read_csv(intraday_data_19_path)
        self.intraday_data_22 = pd.read_csv(intraday_data_22_path)

        # Convert the Date column in daily data to datetime format
        self.daily_data['Date'] = pd.to_datetime(self.daily_data['Date'], format='%d-%m-%Y')
        
        # Concatenate intraday data and convert Date and Time columns
        self.intraday_data = pd.concat([self.intraday_data_19, self.intraday_data_22], ignore_index=True)
        self.intraday_data['Date'] = pd.to_datetime(self.intraday_data['Date'], format='%d-%m-%Y')
        self.intraday_data['Time'] = pd.to_datetime(self.intraday_data['Time'], format='%H:%M:%S').dt.time

    def calculate_30_day_avg(self, stock_name, target_date):
        """
        Calculate the 30-day average volume for a specific stock before a given target date.
        """
        # Filter daily data to the 30 days before the target date
        filtered_data = self.daily_data[(self.daily_data['Stock Name'] == stock_name) & 
                                        (self.daily_data['Date'] < target_date)]
        
        filtered_data = filtered_data.sort_values(by='Date', ascending=False).head(30)
        # Calculate the average volume
        return filtered_data['Volume'].mean()

    def get_avg_volumes(self):
        """
        Get 30-day average volumes for all stocks on specific dates (19th and 22nd April 2024).
        """
        avg_volumes = {}
        for stock_name in self.daily_data['Stock Name'].unique():
            avg_volumes[(stock_name, '19-04-2024')] = self.calculate_30_day_avg(stock_name, pd.Timestamp('19-04-2024'))
            avg_volumes[(stock_name, '22-04-2024')] = self.calculate_30_day_avg(stock_name, pd.Timestamp('22-04-2024'))
        return avg_volumes

    def save_to_csv(self, avg_volumes, all_rolling_data_df, results_df):
        """
        Save the average volume, rolling volume data, and results to CSV files.
        """
        # Convert avg_volumes to a DataFrame format
        avg_volumes_data = []
        for (stock_name, date), avg_volume in avg_volumes.items():
            avg_volumes_data.append({
                'Stock Name': stock_name,
                'Date': date,
                'Average Volume': avg_volume
            })
        
        avg_volumes_df = pd.DataFrame(avg_volumes_data)

        # Save the DataFrames to CSV
        avg_volumes_df.to_csv("average_volumes_19th_22nd_April.csv", index=False)
        for date, df in all_rolling_data_df.items():
            df.to_csv(f"rolling_volume_{date.replace('-', '')}.csv", index=False)
        results_df.to_csv("exceed_volume_results.csv", index=False)

    def analyze_intraday_data(self, avg_volumes):
        """
        Analyze intraday data to calculate cumulative volume and determine when it exceeds the 30-day average.
        """
        # Define the market open time as a time object
        market_open = datetime.strptime("09:15:00", "%H:%M:%S").time()
        
        # Filter data to include only entries after market opens at 9:15 AM
        self.intraday_data = self.intraday_data[self.intraday_data['Time'] >= market_open]

        # Initialize lists to store modified data for CSV output
        all_rolling_data_19 = []
        all_rolling_data_22 = []

        # Initialize a dictionary to store results
        results = {}

        # Process each stock separately
        for stock_name, stock_data in self.intraday_data.groupby('Stock Name'):
            for Date in ['19-04-2024', '22-04-2024']:
                # Filter data for the specific date
                daily_stock_data = stock_data[stock_data['Date'] == pd.Timestamp(Date)]
                
                # Sort by time to ensure the order is correct for rolling calculations
                daily_stock_data = daily_stock_data.sort_values(by='Time')
                
                # Create a cumulative traded volume within a rolling 60-minute window
                daily_stock_data['cumulative_volume'] = daily_stock_data['Last Traded Quantity'].rolling(
                    window=3600, min_periods=1).sum()

                # Get the 30-day average volume
                avg_volume = avg_volumes.get((stock_name, Date), 0)
                
                # Identify when the cumulative volume first exceeds the 30-day average
                exceed_timestamp = daily_stock_data.loc[daily_stock_data['cumulative_volume'] > avg_volume, 'Time']
                
                if Date == '19-04-2024':
                    all_rolling_data_19.append(daily_stock_data)
                elif Date == '22-04-2024':
                    all_rolling_data_22.append(daily_stock_data)

                if not exceed_timestamp.empty:
                    first_exceed_row = daily_stock_data.loc[daily_stock_data['Time'] == exceed_timestamp.iloc[0]]
                    print(f"\nFor {stock_name} on {Date}, the cumulative volume first exceeds the 30-day average at:")
                    print(f"Time: {first_exceed_row['Time'].iloc[0]}, Cumulative Volume: {first_exceed_row['cumulative_volume'].iloc[0]}")

                # Store the first timestamp if it exists, otherwise store None
                if not exceed_timestamp.empty:
                    results[(stock_name, Date)] = exceed_timestamp.iloc[0]
                else:
                    results[(stock_name, Date)] = None

        # Combine all stock data for both dates into single DataFrames
        all_rolling_data_19_df = pd.concat(all_rolling_data_19, ignore_index=True)
        all_rolling_data_22_df = pd.concat(all_rolling_data_22, ignore_index=True)

        return all_rolling_data_19_df, all_rolling_data_22_df, results

    def run_analysis(self):
        """
        Run the complete analysis.
        """
        # Get the average volumes for both dates
        avg_volumes = self.get_avg_volumes()

        # Analyze the intraday data to get the rolling volumes and results
        all_rolling_data_19_df, all_rolling_data_22_df, results = self.analyze_intraday_data(avg_volumes)

        # Combine the rolling data into a dictionary for both dates
        all_rolling_data_df = {
            '19-04-2024': all_rolling_data_19_df,
            '22-04-2024': all_rolling_data_22_df
        }

        # Convert the results into a DataFrame
        results_df = pd.DataFrame.from_dict(results, orient='index', columns=['Time'])

        # Convert the index into a MultiIndex with 'Stock Name' and 'Date'
        results_df.index = pd.MultiIndex.from_tuples(results_df.index, names=['Stock Name', 'Date'])

        # Reset the index to make it a regular DataFrame
        results_df = results_df.reset_index()

        # Save the results to CSV files
        self.save_to_csv(avg_volumes, all_rolling_data_df, results_df)


# Usage
analysis = StockAnalysis("SampleDayData.csv", "19thAprilSampleData.csv", "22ndAprilSampleData.csv")
analysis.run_analysis()
