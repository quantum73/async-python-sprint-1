import os
from pathlib import Path
from unittest.mock import patch, Mock

import pytest

from config import ANALYZE_COMMAND_ERROR_MESSAGE_TEMPLATE
from external.exceptions import AnalyzeError
from tasks import DataCalculationTask


class TestDataCalculationTask:
    @pytest.fixture(autouse=True)
    def temp_dirs(self, tmpdir):
        temp_weather_data_dir = tmpdir.mkdir("weather_data")
        temp_analyze_data_dir = tmpdir.mkdir("analyze_data")
        temp_file_1 = temp_weather_data_dir.join("1.json")
        temp_file_2 = temp_weather_data_dir.join("2.json")
        temp_file_1.write("{}")
        temp_file_2.write("{}")
        return Path(temp_weather_data_dir), Path(temp_analyze_data_dir)

    def test_get_json_paths_with_weather_data(self, temp_dirs):
        temp_weather_data_dir, temp_analyze_data_dir = temp_dirs
        file_names = os.listdir(temp_weather_data_dir)
        temp_file_1 = temp_weather_data_dir / file_names[0]
        temp_file_2 = temp_weather_data_dir / file_names[1]

        with patch('tasks.SAVE_JSON_DIR', temp_weather_data_dir):
            target_result = [temp_file_1, temp_file_2]
            result = DataCalculationTask._get_json_paths_with_weather_data()

            assert isinstance(result, list)
            assert result == target_result

    def test_run_analyze_command(self, temp_dirs):
        temp_weather_data_dir, temp_analyze_data_dir = temp_dirs
        target_weather_path = temp_weather_data_dir / os.listdir(temp_weather_data_dir)[0]
        with patch('tasks.ANALYZE_DIR', temp_analyze_data_dir):
            DataCalculationTask._run_analyze_command(weather_data_path=target_weather_path)

        assert len(os.listdir(temp_analyze_data_dir)) == 1

    @patch("subprocess.Popen")
    def test_run_analyze_command_with_error(self, mocked_popen, temp_dirs):
        process_mock = Mock()
        attrs = {"communicate.return_value": ("output", "error"), "wait.return_value": 1}
        process_mock.configure_mock(**attrs)
        mocked_popen.return_value = process_mock

        temp_weather_data_dir, temp_analyze_data_dir = temp_dirs
        target_weather_path = temp_weather_data_dir / os.listdir(temp_weather_data_dir)[0]
        with patch('tasks.ANALYZE_DIR', temp_analyze_data_dir):
            with pytest.raises(AnalyzeError) as err:
                DataCalculationTask._run_analyze_command(weather_data_path=target_weather_path)

            assert str(err.value) == ANALYZE_COMMAND_ERROR_MESSAGE_TEMPLATE.format(error="error", exit_code=1)

    @patch("tasks.DataCalculationTask._run_analyze_command")
    def test_analyzing_weather(self, mock_run_analyze_command):
        mock_run_analyze_command.return_value = None
        weather_data_path = Path("some/weather/path.json")
        assert DataCalculationTask._analyzing_weather(weather_data_path=weather_data_path) is None

    @patch("tasks.DataCalculationTask._run_analyze_command")
    def test_analyzing_weather_with_analyze_error(self, mock_run_analyze_command):
        mock_run_analyze_command.side_effect = AnalyzeError("OMG")
        weather_data_path = Path("some/weather/path.json")
        assert DataCalculationTask._analyzing_weather(weather_data_path=weather_data_path) is None

    @patch("tasks.DataCalculationTask._run_analyze_command")
    def test_analyzing_weather_with_unexpected_error(self, mock_run_analyze_command):
        mock_run_analyze_command.side_effect = ValueError("Value error")
        weather_data_path = Path("some/weather/path.json")
        assert DataCalculationTask._analyzing_weather(weather_data_path=weather_data_path) is None

    def test_calculate_weather(self, mock_analyze_dir, mock_data_dir, temp_dirs):
        # TODO: ?

        DataCalculationTask.calculate_weather()
