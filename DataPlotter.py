import pandas as pd
from lightweight_charts import Chart
import os
import threading
from time import sleep, time
from datetime import datetime

def set_chart_options(chart):
    # Set chart layout
    chart.layout(background_color='#090008', text_color='#FFFFFF', font_size=16, font_family='Helvetica')

    # Set candle style to match TradingView colors
    chart.candle_style(up_color='#00ff55', down_color='#ed4807', border_up_color='#FFFFFF', border_down_color='#FFFFFF', wick_up_color='#FFFFFF', wick_down_color='#FFFFFF')

    # Set volume configuration to match TradingView colors
    chart.volume_config(up_color='#008000', down_color='#FF0000')
    chart.topbar.textbox('symbol', 'NVDA')
    chart.watermark('1 min', color='rgba(180, 180, 240, 0.7)')


def calculate_vwap(df):
    # Initialize an empty DataFrame with the desired columns
    vwap_df = pd.DataFrame(columns=['time', 'VWAP'])

    # Calculate typical price
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    
    # Calculate dollar volume (typical price * volume)
    df['dollar_volume'] = df['typical_price'] * df['volume']
    
    # Calculate cumulative dollar volume and cumulative volume
    df['cumulative_dollar_volume'] = df['dollar_volume'].cumsum()
    df['cumulative_volume'] = df['volume'].cumsum()
    
    # Calculate VWAP
    df['VWAP'] = df['cumulative_dollar_volume'] / df['cumulative_volume']
    
    # Fill the empty DataFrame with the time and VWAP values
    vwap_df['time'] = df['date']
    vwap_df['VWAP'] = df['VWAP']
    
    # Return the filled DataFrame
    return vwap_df

# Tarvii filtteröidä pois sellaiset tickit joista on jo tehty candle dataa
def filter_new_ticks(df1, df2):
    """
    Compare two DataFrames and return rows from df2 where the hour and minute part of the
    timestamp in 'time' column does not exist in 'date' column of df1.

    Parameters:
    df1 (pd.DataFrame): DataFrame with historical data including a 'date' column.
    df2 (pd.DataFrame): DataFrame with new tick data including a 'time' column.

    Returns:
    pd.DataFrame: Filtered DataFrame containing rows from df2 with unique timestamps.
    """

    # Ensure the 'date' and 'time' columns exist in both DataFrames
    if 'date' not in df1.columns:
        raise ValueError("DataFrame df1 must have a 'date' column")
    if 'time' not in df2.columns:
        raise ValueError("DataFrame df2 must have a 'time' column")

    # Convert 'date' and 'time' columns to datetime format
    df1['date'] = pd.to_datetime(df1['date'], errors='coerce')
    df2['time'] = pd.to_datetime(df2['time'], errors='coerce')

    # Extract hour and minute from 'date' and 'time' columns for comparison
    df1['hour_minute'] = df1['date'].dt.strftime('%H:%M')
    df2['hour_minute'] = df2['time'].dt.strftime('%H:%M')

    # Filter df2 to include only rows with timestamps (hour and minute) not found in df1
    df2_filtered = df2[~df2['hour_minute'].isin(df1['hour_minute'])]

    # Drop the 'hour_minute' column before returning the result
    df2_filtered = df2_filtered.drop(columns=['hour_minute'])

    return df2_filtered




if __name__ == '__main__':

    chart = Chart()
    
    # Calculate VWAP from history data
    df = pd.read_csv('historical_data.csv')
    vwap_df = calculate_vwap(df)
   
    chart.set(df)
    
    set_chart_options(chart)
    line = chart.create_line(name='VWAP', color='red')
    line.set(vwap_df)
    chart.show()

    # Initialize a set to remember which tick timestamps were updated
    updated_timestamps = set()
    last_minute_processed = None
    last_tick_timestamp = None

    while True:

        df2 = pd.read_csv('market_data.csv', header=0)
        
        # Filter new ticks
        df_t = filter_new_ticks(df, df2)

        # Filter out rows with timestamps already in updated_timestamps
        df_t = df_t[~df_t['time'].isin(updated_timestamps)]        

        if not df_t.empty or df.empty:
            last_row_history = df.iloc[-1]
            last_row_tick = df_t.iloc[-1] if not df_t.empty else None
            
            if last_row_tick is not None:
                    # Print the last history data with an explanation
                print(f"Last history: {last_row_history['date']}")

                # Print the last tick data with an explanation
                print(f"Last tick: {last_row_tick['time']}")
                        
            # Print the number of data points found
            print(f"Found {len(df_t)} data point(s) from CSV.")
            
            

            for _, row in df_t.iterrows():
                current_time = row['time']
                current_price = row['price']
                
                # Convert current_time to a string in the format 'YYYY-MM-DD HH:MM'
                current_time_str = current_time.strftime('%Y-%m-%d %H:%M')

                # Only update the chart if the timestamp has not been updated before
                if current_time not in updated_timestamps:
                    chart.update_from_tick(row)
                    updated_timestamps.add(current_time)  # Add the timestamp to the set
                    print(f"Updated chart for timestamp {current_time} with price {current_price}.")
                else:
                    print(f"Skipped updating chart for timestamp {current_time} as it was updated earlier.")

                # Check if this is the first iteration or if a new minute has started
                if last_tick_timestamp is None or current_time_str != last_tick_timestamp.strftime('%Y-%m-%d %H:%M'):
                    
                    print(f"Minute change detected. Updating chart with historical data at {current_time_str}.")
                    sleep(5)
                    # Read only the last row from the CSV file
                    last_row = pd.read_csv('historical_data.csv').tail(1)

                    # Append the last row to the existing DataFrame
                    df = pd.concat([df, last_row], ignore_index=True)

                    # Update the chart with the new data
                    chart.set(df)
                    vwap_df = calculate_vwap(df)
                    line.set(vwap_df)
                
                # Update last_tick_timestamp to the current time
                last_tick_timestamp = current_time
        else:
            sleep(1)