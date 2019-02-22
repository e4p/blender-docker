"""Create a request for the Gooogle Cloud Genomics V2alpha1 pipelines api."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import model


def create_pipeline_request(
    resources_config, job_config, actions, timeout=model.SEVEN_DAYS):
  """Create a pipelines API request."""
  envs = {}
  for vset in job_config.values():
    for v in vset:
      if hasattr(v, 'docker_path'):
        envs[v.name] = os.path.join(model.DATA_DISK_MOUNT, v.docker_path)
      else:
        envs[v.name] = v.value
  return {
      'pipeline': {
          'actions': [a.to_dict() for a in actions],
          'resources': _create_resources(resources_config),
          'environment': envs,
          'timeout': timeout,
      },
      'labels': {
          'minsub': 'v1',
      },
  }


def _create_resources(rconfig):
  """Create the resources payload of the pipelines request."""
  vm = {
      'machineType': rconfig.machine_type,
      'preemptible': False,
      'disks': [
          {
              'name': model.DATA_DISK_NAME,
              'sizeGb': rconfig.disk_size,
          }
      ],
      'serviceAccount': {
          'scopes': rconfig.scopes
      },
  }
  if rconfig.service_account:
    vm['serviceAccount']['email'] = rconfig.service_account
  return {
      'projectId': rconfig.project,
      'regions': [rconfig.region],
      'virtualMachine': vm
  }
