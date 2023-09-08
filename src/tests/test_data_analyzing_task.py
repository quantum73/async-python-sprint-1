from tasks import DataAnalyzingTask
from .mocks import EXAMPLE_DATA_FOR_ANALYZING


class TestDataAnalyzingTask:
    def test_aggregate_analyze_data(self):
        target_template = "Города благоприятные для поездки:\nParis"
        conclusion_template = DataAnalyzingTask.conclusion(aggregated_data=EXAMPLE_DATA_FOR_ANALYZING)
        assert conclusion_template == target_template
