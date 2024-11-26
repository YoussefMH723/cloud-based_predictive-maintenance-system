# Importing libraries
import pymysql
import pickle
import boto3

# Importing model
MODEL_PATH = "model/xgboost.pickle.dat"
model = pickle.load(open(MODEL_PATH, "rb"))

# Setting up database connection 
ENDPOINT = "sensor-db.czee2u6kkp6r.us-east-1.rds.amazonaws.com"
USERNAME = "admin"
PASSWORD = "admin123"
DATABASE_NAME = "sensors"
connection = pymysql.connect(host=ENDPOINT, user=USERNAME, passwd=PASSWORD, db=DATABASE_NAME)

# Defining make_prediction function which takes input data and transform it 
# and returns prediction and prediction probability
def make_prediction(type_, air_temp, process_temp, rotational_speed, torque, tool_wear):
    feature_1 = air_temp*process_temp
    feature_2 = rotational_speed/torque
    feature_3 = rotational_speed*tool_wear
    feature_4 = torque*tool_wear
    if type_ == "M":
        type_m = 1
        type_h, type_l = 0, 0
    elif type == "L":
        type_l = 1
        type_h, type_m = 0, 0
    else:
        type_h = 1
        type_m, type_l = 0, 0
    
    data_row = [[air_temp, process_temp, rotational_speed, torque, tool_wear, feature_1,
                 feature_2, feature_3, feature_4, type_m, type_l, type_h]]
    prediction = model.predict(data_row)[0]
    prediction_proba = model.predict_proba(data_row)[0][1]
    return prediction, prediction_proba

# Defining lambda_handler method which will run as the lambda triggers
def lambda_handler(event, context):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM sensor_data ORDER BY id DESC LIMIT 1")
    row = cursor.fetchall()[0]
    id_, type_, air_temp, process_temp, rotational_speed, torque, tool_wear = row
    prediction, prediction_proba = make_prediction(type_, air_temp, process_temp, rotational_speed, torque, tool_wear)
    query = f"INSERT INTO predictions(m_type, air_temperature, process_temperature, rotational_speed, torque, tool_wear, failure, failure_proba) VALUES (\'{type_}\',{air_temp},{process_temp},{rotational_speed},{torque},{tool_wear},{prediction},{prediction_proba:.4f})"
    print(query)
    cursor.execute(query=query)
    connection.commit()

    if prediction == 1:
        sns_client = boto3.client('sns')
        topic_arn = 'arn:aws:sns:us-east-1:211125711146:failure_sns'
        
        message = 'Machine failure detected!!!!!!!!!!! Immediate attention required.'
        
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject='MACHINE FAILURE ALERT'
        )

    return
