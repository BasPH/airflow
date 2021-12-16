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

"""Starts a Kubernetes Pod for checking a sensor condition."""

from typing import Callable, Optional

from airflow import AirflowException
from airflow.kubernetes import kube_client
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import State


class KubernetesPodSensor(BaseSensorOperator, KubernetesPodOperator):
    """
    The KubernetesPodSensor will start a container which should meet a given condition for Airflow to proceed.
    Two ways for checking the condition are supported:

    1. (default) Airflow monitors the container exit code. If exit code 0 -> success, else -> failure. The
       user must check the condition inside the container and raise a non-zero exit code to fail the container
       if the condition is not met.

    2. For checking the condition in Airflow instead of inside the container, in your container you can write
       JSON output to /airflow/xcom/result.json. With validation_callable set, a sidecar container is added
       to the pod for reading and returning the result. The result can then be checked with a user-provided
       callable. For example:

       ```
       def check_result(output: Dict) -> bool:
           return output["my_value"] > 10

       sense_something = KubernetesPodSensor(..., validation_callable=check_result)
       ```
    """

    def __init__(self, *, validation_callable: Optional[Callable] = None, **kwargs) -> None:
        super().__init__(**kwargs)

        self._validation_callable = validation_callable
        if self._validation_callable is not None:
            self.do_xcom_push = True  # Required for extracting result (even though no XCom is pushed)

    def poke(self, context) -> Optional[str]:
        try:
            if self.in_cluster is not None:
                self.client = kube_client.get_kube_client(
                    in_cluster=self.in_cluster,
                    cluster_context=self.cluster_context,
                    config_file=self.config_file,
                )
            else:
                self.client = kube_client.get_kube_client(
                    cluster_context=self.cluster_context, config_file=self.config_file
                )
            self.pod = self.create_pod_request_obj()
            self.namespace = self.pod.metadata.namespace

            # Add labels to identify the pod
            labels = self.create_labels_for_pod(context)
            label_selector = self._get_pod_identifying_label_string(labels)
            existing_pods = self.client.list_namespaced_pod(self.namespace, label_selector=label_selector)

            if len(existing_pods.items) > 1 and self.reattach_on_restart:
                raise AirflowException(f"More than one pod running with labels: {label_selector}")

            launcher = self.create_pod_launcher()

            if len(existing_pods.items) == 1:
                try_numbers_match = self._try_numbers_match(context=context, pod=existing_pods.items[0])
                final_state, _, result = self.handle_pod_overlap(
                    labels=labels,
                    try_numbers_match=try_numbers_match,
                    launcher=launcher,
                    pod=existing_pods.items[0],
                )
            else:
                self.log.info("Creating pod with labels %s and launcher %s", labels, launcher)
                final_state, _, result = self.create_new_pod_for_operator(labels=labels, launcher=launcher)

            if not self._validation_callable:
                # Check sensor condition based on pod result state
                desired_state = State.SUCCESS
                sensor_result = final_state == desired_state
                if sensor_result:
                    self.log.info("Pod state is %s, returning True", desired_state)
                else:
                    self.log.info(
                        "Sensor requires pod state %s to succeed but found %s, returning False",
                        desired_state,
                        final_state,
                    )
                return sensor_result

            else:
                # Check sensor condition by passing output to callable
                self.log.info("Loaded JSON result: %s", result)
                return self._validation_callable(result)

        except AirflowException as ex:
            raise AirflowException(f"Pod Launching failed: {ex}")
