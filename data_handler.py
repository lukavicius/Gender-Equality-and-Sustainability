import pandas as pd
import wbdata
import datetime
import os


class Data_Handler:
    @staticmethod
    def reshape_long_HDI(df, indicators: dict):
        """
        Converts wide-format year columns into long format for easier filtering.
        """
        id_vars = ['iso3', 'country', 'region']

        # Determine columns that match indicator prefixes
        value_vars = [
            col for col in df.columns
            if any(col.startswith(prefix + "_") for prefix in indicators.keys())
        ]

        long_df = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='metric_year',
            value_name='value'
        )

        # Split 'metric_year' into 'metric' and 'year'
        long_df[['metric', 'year']] = long_df['metric_year'].str.rsplit('_', n=1, expand=True)
        long_df['year'] = long_df['year'].astype(int)
        long_df.drop(columns='metric_year', inplace=True)

        # Add readable metric names
        long_df['metric_name'] = long_df['metric'].map(indicators)

        return long_df

    @staticmethod
    def get_data_HDI(filepath: str, indicators: dict, countries=None, start_year=None, end_year=None):
        """
        Retrieve filtered data based on an indicator dictionary, countries, and year range.

        Args:
            indicators (dict): Dictionary of indicator IDs and readable names.
            countries (list or str): Country or list of countries to filter by.
            start_year (int): Start year for filtering.
            end_year (int): End year for filtering.
            :param filepath: path of the HDI dataset
        """
        # Read CSV safely
        df = pd.read_csv(filepath, encoding="ISO-8859-1")

        # Standardize column names
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
        )
        # Convert to long format using the indicators provided
        long_df = Data_Handler.reshape_long_HDI(df, indicators)

        # Filter by countries
        if countries is not None:
            if isinstance(countries, str):
                countries = [countries]
            long_df = long_df[long_df['country'].str.lower().isin([c.lower() for c in countries])]

        # Filter by year range
        if start_year is not None:
            long_df = long_df[long_df['year'] >= start_year]
        if end_year is not None:
            long_df = long_df[long_df['year'] <= end_year]

        return long_df.reset_index(drop=True)

    @staticmethod
    def get_data_EPI(indicators: dict, countries=None, start_year=None, end_year=None, folder_path='P5_Indicator'):
        """
        Load environmental/social indicators from CSV files and filter by country/year.

        Parameters
        ----------
        indicators : dict
            Dictionary mapping variable abbreviations to readable names, e.g.:
            {"BCA": "Biodiversity Conservation Area", "BER": "Biodiversity Expenditure Ratio"}.
        countries : str | list[str] | None
            Country or list of countries to filter.
        start_year (int): Start year for filtering.
        end_year (int): End year for filtering.
        folder_path : str
            Path to the folder containing CSV files.

        Returns
        -------
        pd.DataFrame
            Long-format DataFrame with columns:
            ['country', 'iso', 'variable', 'variable_name', 'year', 'value']
        """
        all_dfs = []

        for var, var_name in indicators.items():
            filename = os.path.join(folder_path, f"{var}_ind_na.csv")
            if not os.path.exists(filename):
                raise FileNotFoundError(f"File {filename} not found.")

            # Load CSV safely
            df = pd.read_csv(filename)

            # Standardize columns
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

            # Identify year columns for this variable (e.g., bca.ind.1990)
            year_cols = [col for col in df.columns if col.startswith(var.lower() + '.ind.')]

            # Melt wide -> long
            long_df = df.melt(
                id_vars=['iso', 'country'],
                value_vars=year_cols,
                var_name='metric_year',
                value_name='value'
            )

            # Extract year and add metadata
            long_df['year'] = long_df['metric_year'].str.split('.').str[-1].astype(int)
            long_df['variable'] = var
            long_df['variable_name'] = var_name
            long_df = long_df.drop(columns='metric_year')

            all_dfs.append(long_df)

        # Combine multiple indicators
        result = pd.concat(all_dfs, ignore_index=True)

        # Filter by countries
        if countries is not None:
            if isinstance(countries, str):
                countries = [countries]
            result = result[result['country'].str.lower().isin([c.lower() for c in countries])]


        result = result[(result['year'] >= start_year) & (result['year'] <= end_year)]

        return result.reset_index(drop=True)

    @staticmethod
    def get_data_WB(indicators, countries="all", start_year=None, end_year=None):
        """
        Fetch World Bank data.

        Parameters:
            indicators (dict): Mapping from indicator code to descriptive name,
                               e.g. {'NY.GDP.MKTP.CD': 'GDP', 'SP.POP.TOTL': 'Population'}
            countries (list or str): ISO2 country codes like ['US', 'CN'], or 'all'
            start_year (int): Start year (optional)
            end_year (int): End year (optional)

        Returns:
            pd.DataFrame: DataFrame with columns ['Country', 'Year', ...indicators...]
        """
        # Handle date range
        if start_year and end_year:
            date_range = (
                datetime.datetime(start_year, 1, 1),
                datetime.datetime(end_year, 12, 31),
            )
        else:
            date_range = None

        # Fetch from World Bank
        df = wbdata.get_dataframe(
            indicators,
            country=countries,
            date=date_range,
            freq='Y',
            parse_dates=True
        )

        # Reset and clean DataFrame
        df = df.reset_index().rename(columns={"country": "Country", "date": "Year"})

        # Convert Year to integer if parsed as datetime
        if pd.api.types.is_datetime64_any_dtype(df["Year"]):
            df["Year"] = df["Year"].dt.year

        # Reorder columns: Country, Year, then indicators
        indicator_columns = list(indicators.values())
        cols = ["Country", "Year"] + indicator_columns
        df = df[[c for c in cols if c in df.columns]]

        return df
