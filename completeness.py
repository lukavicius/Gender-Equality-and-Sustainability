import pandas as pd
import wbdata
import datetime
import tqdm

class Completeness_Ranker:
    def rank_indicators_by_completeness(source=80, start_year=2000, end_year=2020, top_n=10):
        """
        Fetch all indicators from a source and rank them by data completeness.
        Fetches data in a single call by querying all countries at once.
        """
        # Get all indicators for the source
        indicators = wbdata.get_indicators(source=source)
        indicator_dict = {ind['id']: ind['name'] for ind in indicators}

        print(f"Found {len(indicator_dict)} indicators in source {source}")
        print("Fetching data for all indicators...")

        # Fetch data using the API more efficiently - get all data at once
        try:
            date_range = (datetime.datetime(start_year, 1, 1), datetime.datetime(end_year, 1, 1))

            # Get data for all indicators by passing them as a list to fetch parameter
            all_data = []
            for code in tqdm.tqdm(indicator_dict.keys(), desc="Fetching indicators"):
                try:
                    data = wbdata.get_data(
                        code,
                        date=date_range,
                        freq='Y'
                    )
                    all_data.extend([(code, d['country']['value'], d['date'], d['value'])
                                     for d in data if d['value'] is not None])
                except:
                    continue

            # Convert to dataframe
            df = pd.DataFrame(all_data, columns=['indicator', 'country', 'date', 'value'])

            print(f"Retrieved {len(df)} data points")

            # Calculate completeness for each indicator
            results = []
            total_possible = len(df['country'].unique()) * (end_year - start_year + 1)

            for code, name in tqdm.tqdm(indicator_dict.items(), desc="Calculating completeness"):
                indicator_data = df[df['indicator'] == code]
                count = len(indicator_data)
                completeness = count / total_possible if total_possible > 0 else 0
                results.append((code, name, completeness, count))

            # Create and sort results
            summary = pd.DataFrame(results, columns=["Indicator Code", "Indicator Name", "Completeness", "Data Points"])
            top = summary.sort_values(by="Completeness", ascending=False)

            return top, df

        except Exception as e:
            print(f"Error fetching data: {e}")
            import traceback
            traceback.print_exc()
            return None, None