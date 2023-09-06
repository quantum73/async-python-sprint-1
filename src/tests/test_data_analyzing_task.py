import numpy as np
import pandas as pd

from tasks import DataAnalyzingTask


class TestDataAnalyzingTask:
    def test_aggregate_analyze_data(self):
        target_data_1 = {
            '2022-05-18': {('Moscow', 'Temperature, average'): 13.091, (np.nan, 'No precipitation, hours'): 11.0},
            '2022-05-19': {('Moscow', 'Temperature, average'): 10.727, (np.nan, 'No precipitation, hours'): 5.0},
            '2022-05-20': {('Moscow', 'Temperature, average'): 11.364, (np.nan, 'No precipitation, hours'): 11.0},
            '2022-05-21': {('Moscow', 'Temperature, average'): None, (np.nan, 'No precipitation, hours'): None},
            '2022-05-22': {('Moscow', 'Temperature, average'): None, (np.nan, 'No precipitation, hours'): None},
            'Average': {('Moscow', 'Temperature, average'): 11.73, (np.nan, 'No precipitation, hours'): 9.0},
            'Rating': {('Moscow', 'Temperature, average'): 1, (np.nan, 'No precipitation, hours'): None}
        }
        target_data_2 = {
            '2022-05-18': {('Paris', 'Temperature, average'): 27.091, (np.nan, 'No precipitation, hours'): 5.0},
            '2022-05-19': {('Paris', 'Temperature, average'): 17.727, (np.nan, 'No precipitation, hours'): 7.0},
            '2022-05-20': {('Paris', 'Temperature, average'): 25.364, (np.nan, 'No precipitation, hours'): 5.0},
            '2022-05-21': {('Paris', 'Temperature, average'): None, (np.nan, 'No precipitation, hours'): None},
            '2022-05-22': {('Paris', 'Temperature, average'): None, (np.nan, 'No precipitation, hours'): None},
            'Average': {('Paris', 'Temperature, average'): 23.53, (np.nan, 'No precipitation, hours'): 5.5},
            'Rating': {('Paris', 'Temperature, average'): 15, (np.nan, 'No precipitation, hours'): None}
        }
        example_of_aggregated_data = [
            pd.DataFrame.from_dict(target_data_1),
            pd.DataFrame.from_dict(target_data_2),
        ]
        target_template = "Города благоприятные для поездки:\nParis"
        conclusion_template = DataAnalyzingTask.conclusion(aggregated_data=example_of_aggregated_data)

        assert conclusion_template == target_template
