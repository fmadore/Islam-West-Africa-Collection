from data_fetching import fetch_items_from_set
from text_processing import extract_texts_and_dates, preprocess_texts
from sentiment_analysis import analyze_sentiments
from visualization import create_polarity_time_series
from config import ITEM_SETS


def main():
    """Main function to orchestrate the sentiment analysis and visualization process."""
    # Process for each country in the ITEM_SETS
    for country, sets in ITEM_SETS.items():
        print(f"Processing items for {country}")

        # Fetch items
        items = fetch_items_from_set(sets)

        # Extract texts and their corresponding dates
        texts, dates = extract_texts_and_dates(items)

        # Preprocess texts
        processed_texts = preprocess_texts(texts)

        # Analyze sentiments
        sentiments = analyze_sentiments(processed_texts)

        # Create and save polarity time series plot
        file_name = f'polarity_time_series_{country}.html'
        create_polarity_time_series(sentiments, dates, file_name)
        print(f"Visualization for {country} created: {file_name}")


if __name__ == "__main__":
    main()
