from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
import pandas as pd
import math

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def convert_object_columns(df):
    """
    Convert columns of type 'object' to float, excluding specified columns.
    """
    exclude_columns=['Observatory', 'Data_year', 'agglomeration', 'Zone_complementaire', 'Type_habitat', 'epoque_construction_homogene', 'anciennete_locataire_homogene', 'nombre_pieces_homogene', 'methodologie_production']
    for column in df.select_dtypes(include='object').columns:
        if column not in exclude_columns:
            df[column] = df[column].apply(lambda x: float(x.replace(',', '.')) if isinstance(x, str) else x)
    return df

def select_number_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df[['loyer_moyen', 'surface_moyenne']]

def fill_missing_values_with_median(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        values = sorted(df[col].dropna().tolist())
        median_value = values[math.floor(len(values) / 2)]
        df[[col]] = df[[col]].fillna(median_value)
    return df

@transformer
def execute_transformer_action(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    
    convert_object_columns(df)
    fill_missing_values_with_median(select_number_columns(df))

    action = build_transformer_action(
        df,
        action_type=ActionType.AVERAGE,
        action_code='',  # Enter filtering condition on rows before aggregation
        arguments=['loyer_moyen', 'surface_moyenne'],  # Enter the columns to compute aggregate over
        axis=Axis.COLUMN,
        options={'groupby_columns': ['agglomeration', 'Data_year']},  # Enter columns to group by
        outputs=[
            # The number of outputs below must match the number of arguments
            {'uuid': 'average_loyer_m2', 'column_type': 'number_with_decimals'},
            {'uuid': 'average_surface', 'column_type': 'number_with_decimals'},
        ],
    )

    df = BaseAction(action).execute(df)

    # Sum total for 'nombre_logements' and 'nombre_observations'
    df['total_nombre_logements'] = df.groupby(['agglomeration', 'Data_year'])['nombre_logements'].transform('sum')
    df['total_nombre_observations'] = df.groupby(['agglomeration', 'Data_year'])['nombre_observations'].transform('sum')
    
    df = df[['agglomeration', 'Data_year', 'average_loyer_m2', 'average_surface', 'total_nombre_logements', 'total_nombre_observations']]
    df = df.drop_duplicates()
    
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
