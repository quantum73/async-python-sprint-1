import numpy as np
import pandas as pd


class TestDataAggregationTask:
    def test_aggregate_analyze_data(self, data_aggregation_task_instance):
        city_dataframes = data_aggregation_task_instance.aggregate_analyze_data()

        assert len(city_dataframes) == 1
        assert all([isinstance(item, pd.DataFrame) for item in city_dataframes])

        indexes = city_dataframes[0].to_dict(orient="split").get("index")

        assert indexes is not None
        assert indexes[0][0] == "Moscow"

    def test_save_aggregated_data(self, data_aggregation_task_instance):
        target_data = {
            '2022-05-18': {('Moscow', 'Temperature, average'): 13.091, (np.nan, 'No precipitation, hours'): 11.0},
            '2022-05-19': {('Moscow', 'Temperature, average'): 10.727, (np.nan, 'No precipitation, hours'): 5.0},
            '2022-05-20': {('Moscow', 'Temperature, average'): 11.364, (np.nan, 'No precipitation, hours'): 11.0},
            '2022-05-21': {('Moscow', 'Temperature, average'): None, (np.nan, 'No precipitation, hours'): None},
            '2022-05-22': {('Moscow', 'Temperature, average'): None, (np.nan, 'No precipitation, hours'): None},
            'Average': {('Moscow', 'Temperature, average'): 11.73, (np.nan, 'No precipitation, hours'): 9.0},
            'Rating': {('Moscow', 'Temperature, average'): 1, (np.nan, 'No precipitation, hours'): None}
        }
        example_of_aggregated_data = [pd.DataFrame.from_dict(target_data)]
        data_aggregation_task_instance.save_aggregated_data(aggregated_data=example_of_aggregated_data)

        assert data_aggregation_task_instance.output_csv_path.exists()
