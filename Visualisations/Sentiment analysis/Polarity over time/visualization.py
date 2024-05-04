import pandas as pd
import plotly.express as px
from plotly.offline import plot

def create_polarity_time_series(sentiments, dates, file_name):
    """Create and save a time series plot of sentiment polarity.

    Args:
        sentiments (list): List of polarity scores.
        dates (list): List of dates corresponding to each sentiment score.
        file_name (str): Filename for the output plot.

    Returns:
        None: Outputs an HTML file with the plot.
    """
    df = pd.DataFrame({'Date': dates, 'Polarity': sentiments})

    # Convert date strings to datetime objects, handling different formats
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', infer_datetime_format=True)

    # Remove any rows where dates could not be converted (if any)
    df = df.dropna(subset=['Date'])

    # Group by Date and calculate mean Polarity
    df = df.groupby('Date').mean().reset_index()

    # Create the figure with a range slider
    fig = px.line(df, x='Date', y='Polarity', title="Mean polarity over time")
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(count=5, label="5Y", step="year", stepmode="backward"),
                    dict(count=10, label="10Y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )

    plot(fig, filename=file_name)
