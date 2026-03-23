
def construct_zilliz_key(cognito_id: str, email_id: str, part_id: int, total_parts: int) -> str:
    """
    Constructs a unique key for Zilliz vector database by combining the cognito_id and email_id.

    Inputs:
        cognito_id [str]: The user's Cognito ID.
        email_id [str]: The unique identifier for the email.

    Returns:
        str: A unique key in the format "cognito_id;email_id;part;total_parts".
    """
    return f"{cognito_id};{email_id};{part_id};{total_parts}"


def deconstruct_zilliz_key(zilliz_key: str) -> (str, str, int, int):
    """
    Deconstructs a Zilliz key to retrieve the cognito_id and email_id.

    Inputs:
        zilliz_key [str]: The unique key in the format "cognito_id;email_id".

    Returns:
        tuple: A tuple containing (cognito_id, email_id).
    """
    parts = zilliz_key.split(';', 1)
    if len(parts) != 4:
        raise ValueError("Invalid Zilliz key format. Expected 'cognito_id;email_id;part;total_parts'.")
    return parts[0], parts[1], int(parts[2]), int(parts[3])