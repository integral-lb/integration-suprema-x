import base64
import concurrent
import json
import signal
import time
from datetime import datetime, timezone
import os
import grpc
from flask import Flask, jsonify, request, Config
import threading
import auth_pb2
import connect_pb2
import event_pb2
import face_pb2
import time_pb2
import user_pb2
import re
import operator_pb2

# from biostar.proto.operator_pb2_grpc import Operator
from example.card.card import CardSvc
from example.client.client import GatewayClient
from google.protobuf.json_format import MessageToDict
from example.connect.connect import ConnectSvc

from example.event.event import EventSvc
from example.face.face import FaceSvc
from example.operator.operator import OperatorSvc

from example.time.time import TimeSvc
from example.user.test.testFace import ENROLL_THRESHOLD
from example.user.user import UserSvc
from example.device.device import DeviceSvc
from dotenv import load_dotenv
import os

from operator_pb2_grpc import Operator

load_dotenv()
app = Flask(__name__)

# Sample data
devices = [
    {'id': 1, 'name': 'Device 1', 'status': 'active'},
    {'id': 2, 'name': 'Device 2', 'status': 'inactive'}
]
GATEWAY_CA_FILE = './ca.crt'
GATEWAY_IP = os.getenv('GATEWAY_IP')
GATEWAY_PORT = os.getenv('GATEWAY_PORT')
QUEUE_SIZE = 34


def allow_all_devices(connectSvc):
    try:
        accept_filter = connect_pb2.AcceptFilter(
            allowAll=True
        )

        connectSvc.setAcceptFilter(accept_filter)

        print("Accept filter set: allowing all incoming devices")
        return True

    except grpc.RpcError as e:
        print(f"Failed to set accept filter: {e}")
        return False

def is_connected_status(status):
        return status in [1, 2, "TCP_CONNECTED", "TLS_CONNECTED"]

def get_device_id_device_to_server(connectSvc, expected_device_id=None, wait_seconds=15):
        """
        DEVICE_TO_SERVER mode:
        Device connects to gateway.
        We allow all devices, then check getDeviceList().
        """

        if not allow_all_devices(connectSvc):
            return -1

        end_time = time.time() + wait_seconds

        while time.time() < end_time:
            try:
                devList = connectSvc.getDeviceList()

                for device in devList:
                    device_dict = MessageToDict(device)

                    print("Device found:", device_dict)

                    device_id = device_dict.get("deviceID")
                    status = device_dict.get("status")

                    if not device_id:
                        continue

                    if expected_device_id is not None:
                        if int(device_id) != int(expected_device_id):
                            continue

                    if status is None or is_connected_status(status):
                        print(f"Connected device ID: {device_id}")
                        return int(device_id)

                print("No connected device found yet, waiting...")

            except grpc.RpcError as e:
                print(f"Failed to get device list: {e}")
                return -1

            time.sleep(1)

        print("Device did not connect to gateway")
        return -1

# def get_device_id_by_ip(connectSvc, ip_address, port=51211, useSSL=False):
#
#     devList = connectSvc.getDeviceList()
#     for device in devList:
#         device_dict = MessageToDict(device)
#         if device_dict['IPAddr'] == ip_address:
#             return device_dict['deviceID']
#
#     retry_attempts = 2
#     for attempt in range(retry_attempts):
#         try:
#             print(f"Attempting to connect to device at {ip_address}:{port}, attempt {attempt + 1}")
#             connInfo = connect_pb2.ConnectInfo(IPAddr=ip_address, port=port, useSSL=useSSL)
#             deviceID = connectSvc.connect(connInfo)
#             print(f"Successfully connected to device: {deviceID}")
#             return deviceID
#         except grpc.RpcError as e:
#             print(f"Cannot establish connection on attempt {attempt + 1}: {e}")
#             if attempt >= retry_attempts - 1:
#                 return -1


@app.route('/api/suprema/test-connect', methods=['POST'])
def testconnect():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    connectSvc = ConnectSvc(channel)
    device_service = DeviceSvc(channel)
    device_id = None
    try:
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )

        if device_id is not None:
            device_info = device_service.getInfo(device_id)
            device_info_dict = MessageToDict(device_info)
            response = {
                'success': True,
                'data': device_info_dict
            }
            connectSvc.disconnect([device_id])
            return jsonify(response)
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('No device found with the given IP address')

    except Exception as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))



@app.route('/api/suprema/reboot', methods=['POST'])
def reboot():
    try:
        data = request.get_json() or {}
        expected_device_id = data.get('device_id')
        client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
        channel = client.getChannel()
        connectSvc = ConnectSvc(channel)
        device_service = DeviceSvc(channel)
        device_id = None

        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )
        if device_id is not None or device_id != -1:
            device_service.rebootDevice(device_id)
            connectSvc.disconnect([device_id])
            return jsonify( {'success': True})
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('Could not connect to device')

    except Exception as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))



@app.route('/api/suprema/reset', methods=['POST'])
def reset():
    try:
        data = request.get_json() or {}
        expected_device_id = data.get('device_id')
        client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
        channel = client.getChannel()
        connectSvc = ConnectSvc(channel)
        device_service = DeviceSvc(channel)
        device_id = None

        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )
        if device_id is not None or device_id != -1:
            device_service.clearDatabase(device_id)
            connectSvc.disconnect([device_id])
            return jsonify( {'success': True})
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('Could not connect to device')

    except Exception as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))

@app.route('/api/suprema/devices', methods=['GET'])
def list_devices():
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()

    connectSvc = ConnectSvc(channel)

    try:
        print('in')
        devList = connectSvc.getDeviceList()
        print(devList)
        devices = []
        for device in devList:
            device_dict = MessageToDict(device)
            devices.append(device_dict)
            print(device_dict)

        return jsonify({
            "success": True,
            "count": len(devices),
            "devices": devices
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500
@app.route('/api/suprema/get-date', methods=['POST'])
def getDate():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    connectSvc = ConnectSvc(channel)
    timeSvc = TimeSvc(channel)
    device_id = None
    try:
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )

        if device_id is None or device_id == -1:
            return jsonify({
                'success': False,
                'message': 'Device is not connected to the cloud gateway'
            }), 400

        device_time = timeSvc.getTime(device_id)

        time_dict = datetime.fromtimestamp(
            device_time,
            tz=timezone.utc
        )

        return jsonify({
            'success': True,
            'device_id': device_id,
            'data': time_dict.isoformat()
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/suprema/set-date', methods=['POST'])
def setDate():
    try:
        data = request.get_json() or {}
        expected_device_id = data.get('device_id')
        time = data.get('date')
        timezone = data.get('timezone')
        client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
        channel = client.getChannel()
        connectSvc = ConnectSvc(channel)
        timeSvc = TimeSvc(channel)
        device_id = None
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )
        if device_id is not None and device_id != -1:
            if timezone is not None and timezone != "":
                  tz = int(timezone)
            else :
                tz = int(datetime.now().astimezone().utcoffset().total_seconds())

            try:
                tc = time_pb2.TimeConfig(timeZone=tz, syncWithServer=False)
            except TypeError:
                tc = time_pb2.TimeConfig(time_zone=tz, sync_with_server=False)

            try:
                req = time_pb2.SetConfigRequest(deviceID=device_id, config=tc)
                timeSvc.stub.SetConfig(req)
            except AttributeError:
                try:
                    req = time_pb2.SetTimeConfigRequest(deviceID=device_id, config=tc)
                    timeSvc.stub.SetTimeConfig(req)
                except AttributeError:
                    raise RuntimeError(
                        f"TimeSvc.stub has no SetConfig/SetTimeConfig. Available: {dir(timeSvc.stub)}"
                    )
            timeSvc.setTime(device_id,time)
            connectSvc.disconnect([device_id])
            return jsonify( {'success': True})
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('Could not connect to device')

    except Exception as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


@app.route('/api/suprema/searchdevice')
def searchDevice():
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    connectSvc = ConnectSvc(channel)
    try:
        devList = connectSvc.getDeviceList()
        devList_dict = [MessageToDict(device) for device in devList]
        response = {
            'success': True,
            'data': devList_dict
        }
        return jsonify(response)
    except grpc.RpcError as e:
        raise ValueError(str(e))


@app.route('/api/suprema/get-all-users', methods=['POST'])
def getAllusers():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    user_service = UserSvc(channel)
    connectSvc = ConnectSvc(channel)
    device_id = None
    try:
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )
        if device_id is not None or device_id != -1:
            users = user_service.getList(device_id)  # Assuming getUsers is a method in UserSvc class
            users_dict = [MessageToDict(user) for user in users]
            response = {
                'success': True,
                'data': users_dict
            }
            connectSvc.disconnect([device_id])
            return jsonify(response)
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('Could not connect to device')
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


@app.route('/api/suprema/delete-all-users', methods=['POST'])
def deleteAllusers():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    user_service = UserSvc(channel)
    connectSvc = ConnectSvc(channel)
    device_id = None

    try:
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )

        if device_id is not None or device_id != -1:
            users = user_service.deleteAll(device_id)  # Assuming getUsers is a method in UserSvc class
            users_dict = [MessageToDict(user) for user in users]
            response = {
                'success': True,
                'data': users_dict
            }
            connectSvc.disconnect([device_id])
            return jsonify(response)
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('Could not connect to device')
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))

@app.route('/api/suprema/get-users', methods=['POST'])
def getusers():
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    connectSvc = ConnectSvc(channel)
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')

    user_service = UserSvc(channel)
    user_ids = data.get('user_ids')

    # Ensure user_ids is a list of strings
    print(f"Received user_ids: {user_ids}", flush=True)
    formatted_user_ids = [str(user_id) for user_id in user_ids]
    device_id = None
    try:
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )

        if device_id is not None and device_id != -1:
            users = user_service.getUser(device_id, formatted_user_ids)

            users_dict = [MessageToDict(user) for user in users]
            response = {
                'success': True,
                'data': users_dict
            }
            connectSvc.disconnect([device_id])

            return jsonify(response)
        else:
            connectSvc.disconnect([device_id])
            raise ValueError('Could not connect to device')
    except grpc.RpcError as e:

        connectSvc.disconnect([device_id])
        raise ValueError(str(e))



@app.route('/api/suprema/delete-users', methods=['POST'])
def deleteusers():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    user_ids = data.get('user_ids')
    user_ids_str = [str(user_id) for user_id in user_ids]
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    user_service = UserSvc(channel)
    connectSvc = ConnectSvc(channel)
    device_id = None

    try:
        device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
        )

        if device_id is not None or device_id != -1:
            user_service.delete(device_id, user_ids_str)
            connectSvc.disconnect([device_id])
            return jsonify({'success': True})
        else:
            connectSvc.disconnect([device_id])
            return jsonify({'success': False, 'error': 'Could not get date'})
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


@app.route('/api/suprema/set-user', methods=['POST'])
def enrollFace():
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    userSvc = UserSvc(channel)
    operatorSvc = OperatorSvc(channel)
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    auth_level = data.get('auth_level')
    connectSvc = ConnectSvc(channel)

    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )

    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')
    user_id = data.get('user_id')
    user_name = data.get('user_name')
    face_raw = data.get('face_raw')
    face_template = data.get('face_template')
    face_raw_bytes = base64.b64decode(face_raw)
    newUserHdr = user_pb2.UserHdr(ID=str(user_id))
    newUser = user_pb2.UserInfo(hdr=newUserHdr, setting=user_pb2.UserSetting(), name=user_name)
    try:
        if not face_raw or not face_template:
            raise ValueError("User" + user_id + " must enroll at least one biometric.")

        decoded_bytes_list = []
        for base64_string in face_template:
            try:
                decoded_bytes = base64.b64decode(base64_string)
                decoded_bytes_list.append(decoded_bytes)
            except Exception as e:
                raise ValueError(str(e))

        newUser.setting.cardAuthExtMode = auth_pb2.AUTH_EXT_MODE_CARD_ONLY
        newUser.setting.fingerAuthExtMode = auth_pb2.AUTH_EXT_MODE_FINGERPRINT_ONLY
        newUser.setting.faceAuthExtMode = auth_pb2.AUTH_EXT_MODE_FACE_ONLY
        userSvc.enroll(device_id, [newUser], True)

        # Create FaceData with the raw bytes
        faceData = face_pb2.FaceData(imageData=face_raw_bytes , templates=decoded_bytes_list)
        userFace = user_pb2.UserFace(userID=str(user_id), faces=[faceData])
        face_dict = MessageToDict(userFace)

        userSvc.setFace(device_id, [userFace])

        if auth_level != 0:
            op = auth_pb2.Operator(userID = str(user_id) , level =auth_level )
            op.userID = str(user_id)
            op.level = auth_level
            operatorSvc.add(deviceID=device_id, operators=[op])


        connectSvc.disconnect([device_id])

        return jsonify({'success': True, 'data': 'User enrolled successfully', 'user': face_dict})
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


@app.route('/api/suprema/set-users', methods=['POST'])
def setUsers():
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    connectSvc = ConnectSvc(channel)

    userSvc = UserSvc(channel)
    operatorSvc = OperatorSvc(channel)
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    users = data.get('users')
    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )

    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')
    success_users = []
    failed_users = []
    failure_reasons = {}
    for user in users:
        user_id = user.get('user_id')
        user_name = user.get('user_name')
        face_raw = user.get('face_raw')
        face_template = user.get('face_template')
        auth_level = user.get('auth_level')

        if not face_raw or not face_template:
            reason = "User" + user_id + " must enroll at least one biometric."
            failed_users.append(user_id)
            failure_reasons[user_id] = reason
            continue

        face_raw_bytes = base64.b64decode(face_raw)
        decoded_bytes_list = []
        error_occurred = False
        for base64_string in face_template:
            try:
                decoded_bytes = base64.b64decode(base64_string)
                decoded_bytes_list.append(decoded_bytes)
            except Exception as e:
                reason = f"Error decoding base64 string: {str(e)}"
                error_occurred = True
                failure_reasons[user_id] = reason

        if error_occurred:
            continue

        newUserHdr = user_pb2.UserHdr(ID=str(user_id))
        newUser = user_pb2.UserInfo(hdr=newUserHdr, setting=user_pb2.UserSetting(), name=user_name)
        try:
            userSvc.enroll(device_id, [newUser], True)
            faceData = face_pb2.FaceData(imageData=face_raw_bytes, templates=decoded_bytes_list)
            userFace = user_pb2.UserFace(userID=str(user_id), faces=[faceData])
            userSvc.setFace(device_id, [userFace])
            success_users.append(user_id)
            if auth_level and  auth_level != 0:
                op = auth_pb2.Operator(userID=str(user_id), level=auth_level)
                op.userID = str(user_id)
                op.level = auth_level
                operatorSvc.add(deviceID=device_id, operators=[op])

        except Exception as e:
            reason = f"Error enrolling user: {str(e)}"
            failed_users.append(user_id)
            failure_reasons[user_id] = reason




    connectSvc.disconnect([device_id])
    return jsonify({
        "success_users": success_users,
        "failed_users": failed_users,
        "failure_reasons": failure_reasons
    }), 200


@app.route('/api/suprema/scan-face', methods=['POST'])
def scanFace():
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    faceSvc = FaceSvc(channel)
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    connectSvc = ConnectSvc(channel)
    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )

    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')

    try:
        faceData = faceSvc.scan(device_id, ENROLL_THRESHOLD)
        face_json = MessageToDict(faceData)  # Convert FaceData to JSON
        connectSvc.disconnect([device_id])
        return jsonify({'success': True, 'message': 'scanned successfully', 'data': face_json})
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


@app.route('/api/suprema/enroll-card', methods=['POST'])
def enrollCard():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    userSvc = UserSvc(channel)
    cardSvc = CardSvc(channel)
    data = request.get_json()
    user_id = data.get('user_id')
    connectSvc = ConnectSvc(channel)
    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )

    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')
    newUserHdr = user_pb2.UserHdr(ID=user_id)
    newUser = user_pb2.UserInfo(hdr=newUserHdr, setting=user_pb2.UserSetting())
    try:
        newUser.setting.cardAuthExtMode = auth_pb2.AUTH_EXT_MODE_CARD_ONLY
        newUser.setting.fingerAuthExtMode = auth_pb2.AUTH_EXT_MODE_FINGERPRINT_ONLY
        newUser.setting.faceAuthExtMode = auth_pb2.AUTH_EXT_MODE_FACE_ONLY
        userSvc.enroll(device_id, [newUser], True)
        cardData = cardSvc.scan(deviceID=device_id)
        userCard = user_pb2.UserCard(userID=user_id, cards=[cardData.CSNCardData])
        card_dict = MessageToDict(userCard)
        userSvc.setCard(device_id, [userCard])
        connectSvc.disconnect([device_id])
        return jsonify({'success': True, 'message': 'User enrolled successfully', 'user': card_dict})
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))

def _extract_event_ts(l):
    # try common locations
    v = l.get("timestamp") or l.get("eventTime") or l.get("time") or l.get("occurredTime")
    if v is None:
        return None

    # nested {seconds, nanos}
    if isinstance(v, dict) and "seconds" in v:
        try:
            return int(v["seconds"])
        except Exception:
            return None

    # numeric or numeric string
    try:
        n = int(v)
        if n > 10 ** 12:  # looks like ms epoch
            n //= 1000
        return n
    except Exception:
        pass

    # RFC3339 string
    try:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


@app.route('/api/suprema/get-transactions', methods=['POST'])
def getTransactions():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    start_id = data.get('last_download')
    if start_id is None:
        start_id = 1
    # filters = [event_pb2.EventFilter(startTime=int(start_time))]
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    eventSvc = EventSvc(channel)
    connectSvc = ConnectSvc(channel)
    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )

    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')
    #filters = [event_pb2.EventFilter(TNAKey=i) for i in range(0, 5)]
    try:

        logs = eventSvc.getLog(device_id ,start_id , 1000 )
        # logs = eventSvc.getLogWithFilter(device_id ,1 , 1000 ,filters)
        logs_dict = [MessageToDict(log) for log in logs]
        response = {
            'success': True,
            'data': logs_dict
        }
        # save_logs_to_file(device_id, logs_dict)
        connectSvc.disconnect([device_id])
        return jsonify(response)
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


# @app.route('/api/suprema/get-transactions', methods=['POST'])
# def getTransactions():
#     data = request.get_json()
#     ip_address = data.get('ip')
#     port = data.get('port')
#     start_time = data.get('last_download')
#     if start_time is None:
#         start_time = datetime(2025, 10, 1, 0, 0, 0).timestamp()
#     filters = [event_pb2.EventFilter(startTime=int(start_time))]
#
#     # Create client with increased timeout
#     client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE, timeout=60)  # Increase timeout to 60 seconds
#     channel = client.getChannel()
#     eventSvc = EventSvc(channel)
#     connectSvc = ConnectSvc(channel)
#
#     device_id = get_device_id_by_ip(connectSvc, ip_address, port)
#     if device_id is None or device_id == -1:
#         raise ValueError('Could not connect to device')
#
#     try:
#         # Reduce batch size to 100 instead of 500/1000
#         batch_size = 100
#         all_logs = []
#         start_event_id = 1
#
#         while True:
#             logs = eventSvc.getLogWithFilter(device_id, start_event_id, batch_size, filters)
#             logs_dict = [MessageToDict(log) for log in logs]
#
#             if not logs_dict:
#                 break
#
#             all_logs.extend(logs_dict)
#
#             if len(logs_dict) < batch_size:
#                 break
#
#             # Update start_event_id to the last event ID + 1
#             start_event_id = logs_dict[-1].get('eventID', 0) + 1
#
#         response = {
#             'success': True,
#             'data': all_logs
#         }
#         save_logs_to_file(ip_address, all_logs)
#         connectSvc.disconnect([device_id])
#         return jsonify(response)
#
#     except grpc.RpcError as e:
#         connectSvc.disconnect([device_id])
#         # Log the full error for debugging
#         app.logger.error(f"gRPC error: {str(e)}")
#         raise ValueError(f"Failed to fetch logs: {str(e)}")

def save_logs_to_file(ip_address, logs):
    # Create directory if it doesn't exist
    directory = 'transaction_logs'
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Generate a filename with IP and timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{directory}/{ip_address.replace('.', '-')}_{timestamp}.json"

    # Write logs to the file
    with open(filename, 'w') as f:
        import json
        json.dump(logs, f, indent=4)


@app.route('/api/suprema/transactions-files', methods=['POST'])
def get_transaction_files():
    data = request.get_json()
    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')
    directory = data.get('directory') or 'transaction_logs'  # Default directory if none specified

    # Parse start and end dates and set them to include the entire day range
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    except ValueError:
        return jsonify({'error': 'Invalid start date format. Expected format: Y-m-d'}), 400

    try:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        end_date = end_date.replace(hour=23, minute=59, second=59)  # Set to end of day for accurate comparison
    except ValueError:
        return jsonify({'error': 'Invalid end date format. Expected format: Y-m-d'}), 400

    logs_in_range = []

    # Resolve and check the directory path
    directory_path = os.path.join(os.getcwd(), directory)
    if not os.path.exists(directory_path):
        return jsonify({'error': f'Directory {directory} does not exist.'}), 400

    # Process each file in the directory
    for filename in os.listdir(directory_path):
        match = re.match(r'^([\d-]+)_(\d{8}_\d{6})\.json$', filename)
        if match:
            ip_address = match.group(1).replace('-', '.')
            timestamp_str = match.group(2)
            try:
                file_date = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                # Set file_date to the start of the day (00:00:00)
                file_date = file_date.replace(hour=0, minute=0, second=0)
            except ValueError:
                continue

            # Check if file_date is within start_date and end_date range
            if start_date <= file_date <= end_date:
                file_path = os.path.join(directory_path, filename)
                with open(file_path, 'r') as file:
                    content = json.load(file)
                    logs_in_range.append({
                        'file': filename,
                        'date': file_date.strftime('%Y-%m-%d'),
                        'ip_address': ip_address,
                        'content': content
                    })
        else:
            print(f'Filename does not match expected format: {filename}')

    return jsonify({'success': True, 'data': logs_in_range})


@app.route('/api/suprema/delete-transactions', methods=['POST'])
def deleteTransactions():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    eventSvc = EventSvc(channel)
    connectSvc = ConnectSvc(channel)
    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )

    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')
    try:
        eventSvc.clearLog(device_id)
        response = {
            'success': True,
            'data': "logs deleted"
        }
        connectSvc.disconnect([device_id])
        return jsonify(response)
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))


@app.route('/api/suprema/delete-old-transactions', methods=['POST'])
def deleteOldTransactions():
    data = request.get_json() or {}
    expected_device_id = data.get('device_id')
    client = GatewayClient(GATEWAY_IP, GATEWAY_PORT, GATEWAY_CA_FILE)
    channel = client.getChannel()
    eventSvc = EventSvc(channel)
    connectSvc = ConnectSvc(channel)
    device_id = None
    device_id = get_device_id_device_to_server(
            connectSvc=connectSvc,
            expected_device_id=expected_device_id,
            wait_seconds=30
    )
    if device_id is  None or device_id == -1:
        raise ValueError('Could not connect to device')
    try:
        eventSvc.clearLog(device_id)
        response = {
            'success': True,
            'data': "logs deleted"
        }
        connectSvc.disconnect([device_id])
        return jsonify(response)
    except grpc.RpcError as e:
        connectSvc.disconnect([device_id])
        raise ValueError(str(e))

if __name__ == '__main__':
    app.run(debug=True,port=5000, host="0.0.0.0")
