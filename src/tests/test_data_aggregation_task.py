import pandas as pd

from .mocks import EXAMPLE_DATA_FOR_AGGREGATE


class TestDataAggregationTask:
    def test_aggregate_analyze_data(self, data_aggregation_task_instance):
        city_dataframes = data_aggregation_task_instance.aggregate_analyze_data()

        assert len(city_dataframes) == 1
        assert all([isinstance(item, pd.DataFrame) for item in city_dataframes])

        indexes = city_dataframes[0].to_dict(orient="split").get("index")

        assert indexes is not None
        assert indexes[0][0] == "Moscow"

    def test_save_aggregated_data(self, data_aggregation_task_instance):
        data_aggregation_task_instance.save_aggregated_data(aggregated_data=EXAMPLE_DATA_FOR_AGGREGATE)
        assert data_aggregation_task_instance.output_csv_path.exists()
