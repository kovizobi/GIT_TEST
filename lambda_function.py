from io import StringIO
import json
import boto3
# Note: to make the Pandas library available to provide layer of runtime 
# environment add Layers -> Add Layer -> AWS layers -> AWSSDKPandas-Python111
import pandas as pd 


def taxi_trips_transformations(taxi_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Perform transformations on the taxi trip data.

    Args:
        taxi_trips (pd.DataFrame): DataFrame containing raw taxi trip data.

    Returns:
        pd.DataFrame: Cleaned and transformed taxi trip data.
    """
    if not isinstance(taxi_trips, pd.DataFrame):
        raise TypeError("taxi_trips is not a valid Pandas dataframe")

    taxi_trips.drop([
        "pickup_census_tract", "dropoff_census_tract", 
        "pickup_centroid_location", "dropoff_centroid_location"
    ], axis=1, inplace=True)
    taxi_trips.dropna(inplace=True)
    taxi_trips.rename(columns={
        "pickup_community_area": "pickup_community_area_id",
        "dropoff_community_area": "dropoff_community_area_id"
    }, inplace=True)
    taxi_trips["datetime_for_weather"] = pd.to_datetime(taxi_trips["trip_start_timestamp"]).dt.floor("h")
    return taxi_trips


def update_taxi_trips_with_master_data(
    taxi_trips: pd.DataFrame, 
    payment_type_master: pd.DataFrame, 
    company_master: pd.DataFrame
) -> pd.DataFrame:
    """
    Join taxi trip data with company and payment type master data.

    Args:
        taxi_trips (pd.DataFrame): Transformed taxi trip data.
        payment_type_master (pd.DataFrame): Master data for payment types.
        company_master (pd.DataFrame): Master data for companies.

    Returns:
        pd.DataFrame: Taxi trip data with master IDs and without text columns.
    """
    taxi_trips_id = taxi_trips.merge(payment_type_master, on="payment_type")
    taxi_trips_id = taxi_trips_id.merge(company_master, on="company")
    taxi_trips_id.drop(["payment_type", "company"], axis=1, inplace=True)
    return taxi_trips_id


def update_master(
    taxi_trips: pd.DataFrame,
    master: pd.DataFrame, 
    id_column: str,
    value_column: str
) -> pd.DataFrame:
    """
    Update the master DataFrame with new unique values from taxi_trips.

    Args:
        taxi_trips (pd.DataFrame): DataFrame with daily taxi trip data.
        master (pd.DataFrame): Master DataFrame to be updated.
        id_column (str): Column name for IDs in the master DataFrame.
        value_column (str): Column name for values in both DataFrames.

    Returns:
        pd.DataFrame: Updated master DataFrame.
    """
    max_id = master[id_column].max()
    new_values_list = list(set(taxi_trips[value_column].values) - set(master[value_column].values))
    new_values_df = pd.DataFrame({
        id_column: range(max_id + 1, max_id + 1 + len(new_values_list)),
        value_column: new_values_list 
    })
    updated_master = pd.concat([master, new_values_df], ignore_index=True)
    return updated_master


def transform_weather_data(weather_data: dict) -> pd.DataFrame:
    """
    Transform raw weather JSON data into a structured DataFrame.

    Args:
        weather_data (dict): JSON data from Open-Meteo API.

    Returns:
        pd.DataFrame: Transformed weather data.
    """
    weather_data_filtered = {
        "datetime": weather_data["hourly"]["time"],
        "tempretaure": weather_data["hourly"]["temperature_2m"],
        "wind_speed": weather_data["hourly"]["wind_speed_10m"],
        "rain": weather_data["hourly"]["rain"],
        "precipitation": weather_data["hourly"]["precipitation"]
    }
    weather_df = pd.DataFrame(weather_data_filtered)
    weather_df["datetime"] = pd.to_datetime(weather_df["datetime"])
    return weather_df


def read_csv_from_s3(bucket: str, path: str, filename: str) -> pd.DataFrame:
    """
    Download cscv file from S3 bucket.
    
    Parameters:
    bucket (str): The bucket where the files at.
    path (str): The folder to the file.
    filename (str): Original file name.

    Returns:
        pd:DataFrame - the dataframe of the downloaded file
    """
    s3 = boto3.client("s3") 
    full_path = f"{path}{filename}"
    object = s3.get_object(Bucket=bucket, Key=full_path)
    object = object['Body'].read().decode('utf-8')
    output_df = pd.read_csv(StringIO(object))
    return output_df  


def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Reads and parses a JSON file from an S3 bucket.

    Parameters:
        bucket (str): Name of the S3 bucket.
        key (str): Key (path) to the JSON file in the bucket.

    Returns:
        dict: Parsed JSON content as a Python dictionary.
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"]
    return json.loads(content.read())


def uploada_dataframe_to_s3(dataframe: pd.DataFrame, bucket: str, path: str):
    """
    Upload a DataFrame as a CSV file to S3.

    Args:
        dataframe (pd.DataFrame): The DataFrame to upload.
        bucket (str): Target S3 bucket.
        path (str): Target S3 path including filename.
    """
    s3 = boto3.client("s3")
    buffer = StringIO()
    dataframe.to_csv(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=path, Body=buffer.getvalue())


def upload_master_data_to_s3(bucket: str, path: str, file_type: str, dataFrame: pd.DataFrame):
    """
    Upload updated master data to S3 and preserve previous version.

    Args:
        bucket (str): S3 bucket name.
        path (str): Path in the bucket.
        file_type (str): Either 'company' or 'payment_type'.
        dataFrame (pd.DataFrame): The master DataFrame to upload.
    """
    s3 = boto3.client("s3")
    master_file_path = f"{path}{file_type}_master.csv"
    previous_path = f"transformed_data/master_table_previous_version/{file_type}_master_previous_version.csv"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": master_file_path}, Key=previous_path)
    uploada_dataframe_to_s3(dataframe=dataFrame, bucket=bucket, path=master_file_path)


def upload_and_move_file_on_s3(
    dataframe: pd.DataFrame,
    datetime_col: str,
    bucket: str,
    file_type: str,
    filename: str,
    source_path: str,
    target_path_raw: str,
    target_path_transformed: str
):
    """
    Upload a transformed file and archive the original raw file in S3.

    Args:
        dataframe (pd.DataFrame): Transformed DataFrame to upload.
        datetime_col (str): Column name containing datetime info.
        bucket (str): S3 bucket name.
        file_type (str): Type of file (e.g., 'taxi', 'weather').
        filename (str): Original file name.
        source_path (str): Current S3 location of raw file.
        target_path_raw (str): Archive location for raw file.
        target_path_transformed (str): Destination for transformed file.
    """
    s3 = boto3.client("s3")
    formatted_date = dataframe[datetime_col].iloc[0].strftime("%Y-%m-%d")
    new_path = f"{target_path_transformed}{file_type}_{formatted_date}.csv"
    uploada_dataframe_to_s3(dataframe=dataframe, bucket=bucket, path=new_path)
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": f"{source_path}{filename}"}, Key=f"{target_path_raw}{filename}")
    s3.delete_object(Bucket=bucket, Key=f"{source_path}{filename}")
    # Note: creating an empty placeholder file named .keep in an S3 
    # folder to ensure the folder structure is preserved, especially 
    # when all other files are moved or deleted
    s3.put_object(Bucket=bucket, Key=f"{source_path}.keep", Body=b"")
    print(f"{file_type} data processed and folder retained.")


def lambda_handler(event, context):
    """
    AWS Lambda handler to process raw taxi and weather data from an S3 bucket.

    - Loads master data for payment type and company from S3.
    - Processes raw taxi and weather JSON files.
    - Transforms and enriches taxi data with master data.
    - Uploads processed files to appropriate S3 paths.
    - Updates and saves master datasets back to S3.

    Parameters:
        event: AWS Lambda uses this parameter to pass event data to the handler.
        context: AWS Lambda uses this parameter to provide runtime information.
    """
    s3 = boto3.client("s3")
    bucket = "cubix-chicago-taxi-ke"

    # S3 folder paths
    raw_taxi_trips_folder = "raw_data/to_processed/taxi_data/"
    raw_weather_folder = "raw_data/to_processed/weather_data/"
    target_taxi_trips_folder = "raw_data/processed/taxi_data/"
    target_weather_folder = "raw_data/processed/weather_data/"
    transformed_taxi_trips_folder = "transformed_data/taxi_trips/"
    transformed_weather_folder = "transformed_data/weather/"
    payment_type_master_folder = "transformed_data/payment_type/"
    company_master_folder = "transformed_data/company/"
    payment_type_master_file_name = "payment_type_master.csv"
    company_master_file_name = "company_master.csv"

    # Load master data
    payment_type_master = read_csv_from_s3(
        bucket=bucket,
        path=payment_type_master_folder,
        filename=payment_type_master_file_name
    )
    company_master = read_csv_from_s3(
        bucket=bucket,
        path=company_master_folder,
        filename=company_master_file_name
    )

    # Process taxi data files
    for file in s3.list_objects(Bucket=bucket, Prefix=raw_taxi_trips_folder).get("Contents", []):
        taxi_trip_key = file["Key"]
        filename = taxi_trip_key.split("/")[-1]

        if filename and filename.endswith(".json"):
            
            # response = s3.get_object(Bucket=bucket, Key=taxi_trip_key)
            # content = response["Body"]
            # taxi_trip_data_json = json.loads(content.read())
            taxi_trips_data_json = read_json_from_s3(Bucket=bucket, Key=taxi_trip_key)

            taxi_trips_data_raw = pd.DataFrame(taxi_trip_data_json)
            taxi_trips_transformed = taxi_trips_transformations(taxi_trips_data_raw)

            # Update and merge master data
            company_master_updated = update_master(
                taxi_trips_transformed, company_master, "company_id", "company"
            )
            payment_type_master_updated = update_master(
                taxi_trips_transformed, payment_type_master, "payment_type_id", "payment_type"
            )

            taxi_trips = update_taxi_trips_with_master_data(
                taxi_trips_transformed, payment_type_master_updated, company_master_updated
            )

            upload_and_move_file_on_s3(
                dataframe=taxi_trips,
                datetime_col="datetime_for_weather",
                bucket=bucket,
                file_type="taxi",
                filename=filename,
                source_path=raw_taxi_trips_folder,
                target_path_raw=target_taxi_trips_folder,
                target_path_transformed=transformed_taxi_trips_folder
            )
            print("taxi_trips has been uploaded")

            upload_master_data_to_s3(
                bucket=bucket,
                path=payment_type_master_folder,
                file_type="payment_type",
                dataFrame=payment_type_master_updated
            )
            upload_master_data_to_s3(
                bucket=bucket,
                path=company_master_folder,
                file_type="company",
                dataFrame=company_master_updated
            )

    # Process weather data files
    for file in s3.list_objects(Bucket=bucket, Prefix=raw_weather_folder).get("Contents", []):
        weather_key = file["Key"]
        filename = weather_key.split("/")[-1]

        if filename and filename.endswith(".json"):

            # response = s3.get_object(Bucket=bucket, Key=weather_key)
            # content = response["Body"]
            # weather_data_json = json.loads(content.read())
            weather_data_json = read_json_from_s3(Bucket=bucket, Key=taxi_trip_key)

            weather_data = transform_weather_data(weather_data_json)

            upload_and_move_file_on_s3(
                dataframe=weather_data,
                datetime_col="datetime",
                bucket=bucket,
                file_type="weather",
                filename=filename,
                source_path=raw_weather_folder,
                target_path_raw=target_weather_folder,
                target_path_transformed=transformed_weather_folder
            )
            print("weather has been uploaded")



