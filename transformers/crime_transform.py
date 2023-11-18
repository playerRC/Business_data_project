if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def convert_object_columns(df):
    """
    Convert columns of type 'object' to float, excluding specified columns.
    """
    exclude_columns=['classe', 'Code.département', 'unité.de.compte']
    for column in df.select_dtypes(include='object').columns:
        if column not in exclude_columns:
            df[column] = df[column].apply(lambda x: float(x.replace(',', '.')) if isinstance(x, str) else x)
    return df


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    convert_object_columns(df)

    df['annee'] = df['annee'].apply(lambda x: x + 2000)
    
    df = df[['Code.département', 'annee', 'classe', 'faits', 'tauxpourmille']]

    df['tauxpourmille'].round(2)

    df.rename(columns={"Code.département": "code_departement"}, inplace=True)
    df.rename(columns={"faits": "nb_faits"}, inplace=True)
    df.rename(columns={"tauxpourmille": "taux_pour_1000"}, inplace=True)


    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
