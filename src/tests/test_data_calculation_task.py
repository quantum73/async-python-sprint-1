import os


class TestDataCalculationTask:
    def test_calculate_weather(self, data_calculation_task_instance):
        data_calculation_task_instance.calculate_weather()

        assert len(os.listdir(data_calculation_task_instance.input_weather_data_dir)) == 1
        assert len(os.listdir(data_calculation_task_instance.output_analyze_dir)) == 1
        assert set(os.listdir(data_calculation_task_instance.output_analyze_dir)) == {"MOSCOW.json"}
