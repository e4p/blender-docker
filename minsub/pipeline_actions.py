"""Create actions for minsub."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import textwrap

import model

# Images used for minsub; commonly used images are cached by Cloud
# so using generic tags will ensure faster loading than specific tags
# that identify potentially uncached image versions.
DEBIAN_IMAGE = 'debian:stable-slim'
CLOUD_SDK_IMAGE = 'google/cloud-sdk:slim'

GENERIC_BASH_SCRIPT_CMD = textwrap.dedent("""\
    set -o errexit
    set -o nounset
    set -o pipefail

    %s
""")


class GenericAction(object):
  """Base class for the definition of actions."""

  def __init__(self, job_config):
    self.mount_point = model.DATA_DISK_MOUNT
    self.mount_disk_name = model.DATA_DISK_NAME
    self.config = job_config
    self.name = ''
    self.image = DEBIAN_IMAGE
    self.entrypoint_override = '/bin/bash'
    self.timeout = model.ONE_DAY
    self.envs = {}
    self.flags = []

  def make_envs(self):
    """Set environmental variables, optionally override this."""
    envs = self.envs.copy()
    return envs

  def make_commands(self):
    raise NotImplementedError('Derived class must implement this')

  def to_dict(self):
    """Convert the generic action into an action request parameter."""
    envs = self.make_envs()
    cmds = self.make_commands()
    d = {
        'name': self.name,
        'imageUri': self.image,
        'commands': cmds,
        'environment': envs,
        'flags': self.flags,
        'mounts': [
            {
                'disk': self.mount_disk_name,
                'path': self.mount_point,
                'readOnly': False,
            },
        ],
        'timeout': self.timeout
    }
    if self.entrypoint_override:
      d['entrypoint'] = self.entrypoint_override
    return d

  def to_json(self, pretty=False):
    """Convert to a json string."""
    if pretty:
      return json.dumps(self.to_dict(), indent=4, sort_keys=True)
    else:
      return json.dumps(self.to_dict())


class LocalizeAction(GenericAction):
  """Localize files from GCS."""

  def __init__(self, jconfig):
    super(LocalizeAction, self).__init__(jconfig)
    self.name = 'localize'
    self.image = CLOUD_SDK_IMAGE

  def make_commands(self):
    """Create the localize command from the job config."""
    copy_commands = []
    for ninput in self.config.inputs:
      dst = os.path.join(self.mount_point, ninput.docker_path)
      src = ninput.value
      copy_commands.append('gsutil -mq cp "%s" "%s"' % (src, dst))
    for rinput in self.config.recursive_inputs:
      dst = os.path.join(self.mount_point, rinput.docker_path)
      src = rinput.value
      copy_commands.append('gsutil -mq rsync -r "%s" "%s"' % (src, dst))
    return ['-c', GENERIC_BASH_SCRIPT_CMD % '\n'.join(copy_commands)]


class DelocalizeAction(GenericAction):
  """Delocalize files to GCS."""

  def __init__(self, jconfig):
    super(DelocalizeAction, self).__init__(jconfig)
    self.name = 'delocalize'
    self.image = CLOUD_SDK_IMAGE

  def make_commands(self):
    """Create the localize command from the job config."""
    copy_commands = []
    for nout in self.config.outputs:
      src = os.path.join(self.mount_point, nout.docker_path)
      dst = nout.value
      copy_commands.append('gsutil -mq cp "%s" "%s"' % (src, dst))
    for rout in self.config.recursive_outputs:
      src = os.path.join(self.mount_point, rout.docker_path)
      dst = rout.value
      copy_commands.append('gsutil -mq rsync -r "%s" "%s"' % (src, dst))
    return ['-c', GENERIC_BASH_SCRIPT_CMD % '\n'.join(copy_commands)]


class UserAction(GenericAction):
  """Localize files from GCS."""

  def __init__(self, jconfig, action_name, docker_image, command):
    super(UserAction, self).__init__(jconfig)
    self.name = action_name
    self.image = docker_image
    self.command = command

  def make_commands(self):
    """Create the localize command from the job config."""
    return self.command
