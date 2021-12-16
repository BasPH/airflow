#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
from typing import Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.sensors.kubernetes_pod import KubernetesPodSensor

with DAG(
    "kubernetes_pod_sensor",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
    tags=["example"],
) as dag:

    def check(output: Dict) -> bool:
        result = output["result"] is True

        result_msgs = {
            True: "Result successful. Returning True.",
            False: "Result not successful. Returning False.",
        }
        print(result_msgs[result])

        return result

    sense_pod_validation = KubernetesPodSensor(
        task_id="sense_pod_validation",
        image="alpine",
        name="testme",
        cmds=[
            "sh",
            "-c",
            'echo \'{"result": '
            + ("true" if datetime.datetime.now().minute % 2 == 0 else "false")
            + "}' > /airflow/xcom/return.json",
        ],
        validation_callable=check,
        mode="reschedule",
    )

    sense_pod_status = KubernetesPodSensor(
        task_id="sense_pod_status",
        image="alpine",
        name="testme",
        cmds=["sh", "-c", "exit 0" if datetime.datetime.now().minute % 2 == 0 else "exit 1"],
        mode="reschedule",
    )

    success = BashOperator(bash_command="echo SUCCESS!", task_id="success")
    [sense_pod_validation, sense_pod_status] >> success
