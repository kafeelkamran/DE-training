import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from metrix_repo.analyzers.churn_analyzer import calculate_churn_rate

def test_churn_rate_calculation():
    # Create a sample DataFrame
    data = {
        'customer_id': [1, 2, 3, 4],
        'status': ['active', 'churned', 'active', 'churned']
    }
    df = pd.DataFrame(data)

    # Expected churn rate: 2 churned out of 4 total = 0.5
    result = calculate_churn_rate(df)
    assert result == 0.5
