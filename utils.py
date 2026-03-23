
def construct_zilliz_key(cognito_id: str, email_id: str) -> str:
    """
    Constructs a unique key for Zilliz vector database by combining the cognito_id and email_id.

    Inputs:
        cognito_id [str]: The user's Cognito ID.
        email_id [str]: The unique identifier for the email.

    Returns:
        str: A unique key in the format "cognito_id;email_id".
    """
    return f"{cognito_id};{email_id}"


def deconstruct_zilliz_key(zilliz_key: str) -> (str, str):
    """
    Deconstructs a Zilliz key to retrieve the cognito_id and email_id.

    Inputs:
        zilliz_key [str]: The unique key in the format "cognito_id;email_id".

    Returns:
        tuple: A tuple containing (cognito_id, email_id).
    """
    parts = zilliz_key.split(';', 1)
    if len(parts) != 2:
        raise ValueError("Invalid Zilliz key format. Expected 'cognito_id;email_id'.")
    return parts[0], parts[1]