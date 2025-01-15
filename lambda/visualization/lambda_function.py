import os

# Set Matplotlib config directory to /tmp (must be set before importing Matplotlib)
os.environ["MPLCONFIGDIR"] = "/tmp"

import json
import boto3

from boto3.dynamodb.conditions import Key
import geopandas as gpd
from shapely.geometry import box
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from io import BytesIO
import contextily as ctx

# Initialize the DynamoDB client
dynamodb = boto3.resource("dynamodb")

# Initialize S3 bucket
s3 = boto3.client("s3")

# DynamoDB table
table = dynamodb.Table("Sensordata")

# S3 bucket name
BUCKET_NAME = "heatmap-bucket"
output_path = "heatmaps/sensor_heatmap.png"


square_size_lat = 0.009
square_size_lon = square_size_lat / 0.7


def fetch_data_from_dynamodb():
    """
    Fetch latest records from the DynamoDB table.
    Returns a list of records with sensor data.
    """
    response = table.scan(ProjectionExpression="sensor_id")
    sensor_ids = {item["sensor_id"] for item in response.get("Items", [])}

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="sensor_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        sensor_ids.update({item["sensor_id"] for item in response.get("Items", [])})

    latest_data = []
    for sensor_id in sensor_ids:
        query_response = table.query(
            KeyConditionExpression=Key("sensor_id").eq(sensor_id),
            ScanIndexForward=False,
            Limit=1,
        )
        latest_data.extend(query_response.get("Items", []))

    return latest_data


def create_heatmap(data, output_path):
    """
    Create a heatmap based on DynamoDB data and save it to the specified path.
    """
    latitudes = [item["location"]["lat"] for item in data]
    longitudes = [item["location"]["lon"] for item in data]
    temperatures = [item["measurements"]["temperature"] for item in data]

    geometry = [
        box(
            float(lon) - square_size_lon / 2,
            float(lat) - square_size_lat / 2,
            float(lon) + square_size_lon / 2,
            float(lat) + square_size_lat / 2,
        )
        for lon, lat in zip(longitudes, latitudes)
    ]
    geo_df = gpd.GeoDataFrame(
        {"geometry": geometry, "temperature": temperatures}, crs="EPSG:4326"
    )

    cmap = plt.cm.viridis
    norm = Normalize(vmin=min(temperatures), vmax=max(temperatures))

    fig, ax = plt.subplots(figsize=(12, 8))
    geo_df.plot(
        ax=ax,
        color=[cmap(norm(float(temp))) for temp in temperatures],
        alpha=0.5,
        edgecolor="black",
    )

    ctx.add_basemap(ax, crs="EPSG:4326", source=ctx.providers.Esri.WorldImagery)

    sm = ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical", fraction=0.02, pad=0.04)
    cbar.set_label("Temperature (°C)")

    ax.set_title(
        "Satellite Heatmap with Temperature-Based Colored Squares", fontsize=15
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    buffer = BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight")
    buffer.seek(0)

    s3.upload_fileobj(buffer, BUCKET_NAME, output_path)
    buffer.close()


def lambda_handler(event, context):
    """
    Lambda function to create a heatmap and store it in an S3 bucket.
    """
    try:
        data = fetch_data_from_dynamodb()
        create_heatmap(data, output_path)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Plot created and uploaded to S3", "s3_path": output_path}
            ),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
