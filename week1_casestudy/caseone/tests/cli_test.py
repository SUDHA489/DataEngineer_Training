from caseone.cli_tool import clean_and_validate
import pandas as pd
import pytest


@pytest.mark.parametrize("data", [
    # Case 1: Valid entry and exit times, positive paid_amount
    {
        'vehicle_id': ['V001'],
        'entry_time': ['2024-07-18 08:00:00'],
        'exit_time': ['2024-07-18 10:00:00'],
        'paid_amount': [100]
    },
    # Case 2: Another valid row
    {
        'vehicle_id': ['V002'],
        'entry_time': ['2024-07-18 09:00:00'],
        'exit_time': ['2024-07-18 09:30:00'],
        'paid_amount': [50]
    }
])
def test_clean_and_validate_valid_cases(data):
    df = pd.DataFrame(data)
    result = clean_and_validate(df)
    assert not result.isnull().any().any()


@pytest.mark.parametrize("data, expected_error", [
    # Missing vehicle_id column
    (
        {
            'entry_time': ['2024-07-18 08:00:00'],
            'exit_time': ['2024-07-18 09:00:00'],
            'paid_amount': [50]
        },
        "Missing 'vehicle_id'"
    ),

    # Null vehicle_id
    (
        {
            'vehicle_id': [None],
            'entry_time': ['2024-07-18 08:00:00'],
            'exit_time': ['2024-07-18 09:00:00'],
            'paid_amount': [50]
        },
        "Missing or null values in 'vehicle_id'"
    ),

    # Invalid entry_time
    (
        {
            'vehicle_id': ['V001'],
            'entry_time': ['invalid'],
            'exit_time': ['2024-07-18 09:00:00'],
            'paid_amount': [50]
        },
        "Invalid datetime values found in 'entry_time'"
    ),

    # exit_time before entry_time
    (
        {
            'vehicle_id': ['V001'],
            'entry_time': ['2024-07-18 10:00:00'],
            'exit_time': ['2024-07-18 08:00:00'],
            'paid_amount': [50]
        },
        "exit_time cannot be earlier than entry_time"
    ),

    # Negative paid_amount
    (
        {
            'vehicle_id': ['V001'],
            'entry_time': ['2024-07-18 08:00:00'],
            'exit_time': ['2024-07-18 09:00:00'],
            'paid_amount': [-10]
        },
        "paid_amount cannot have negative"
    )
])
def test_clean_and_validate_invalid_cases(data, expected_error):
    df = pd.DataFrame(data)
    with pytest.raises(ValueError, match=expected_error):
        clean_and_validate(df)




from caseone.cli_tool import ETL

def test_etl(tmp_path):

    # 1. Prepare input CSV
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "vehicle_id,entry_time,exit_time,paid_amount,timestamp\n"
        "V001,2024-07-18 08:00:00,2024-07-18 10:00:00,100,2024-07-18 10:00:00\n"
        "V002,2024-07-18 09:00:00,2024-07-18 11:00:00,50,2024-07-18 11:00:00\n"
    )

    # 2. Define output Parquet path
    output_file = tmp_path / "output.parquet"

    # 3. Run ETL
    ETL(str(input_csv), str(output_file))

    # 4. Read result and assert
    df = pd.read_parquet(output_file)
    assert len(df) == 2
    assert 'vehicle_id' in df.columns
    assert df['paid_amount'].sum() == 150