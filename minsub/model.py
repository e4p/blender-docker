"""Store constants and models for minsub."""

import collections

DATA_DISK_NAME = 'minsubdisk'
DATA_DISK_MOUNT = '/mnt/data'

# Interval strings for timeout.
ONE_HOUR = '3600s'
TWO_HOURS = '7200s'
ONE_DAY = '86400s'
SEVEN_DAYS = '604800s'

DEFAULT_DISK_SIZE = 200
DEFAULT_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
DEFAULT_MACHINE_TYPE = 'n1-standard-2'


class ResourcesConfig(
    collections.namedtuple('ResourcesConfig', [
        'project',
        'region',
        'machine_type',
        'disk_size',
        'service_account',
        'scopes',
    ])):
  """File parameter to be automatically localized or de-localized.

  Input files are automatically localized to the pipeline VM's local disk.

  Output files are automatically de-localized to a remote URI from the
  pipeline VM's local disk.

  Attributes:
    project (str): a Google Cloud Platform project ID.
    region (str): a GCE compute zone where the work will be done.
    machine_type (str): (optional) the machine type to use, defaults to
        n1-standard-2.
    disk_size (int): (optional) the size in GB to allocate for the working data
        disk, defaults to 200 GB.
    service_account (str): (optional) the service account email, If none is
        provided, pipelines API defaults to the compute service account.
    scopes (str|list<str>): (optional) the scopes to use for the launched job,
        defaults to https://www.googleapis.com/auth/cloud-platform,
  """
  __slots__ = ()

  def __new__(cls,
              project,
              region,
              machine_type=DEFAULT_MACHINE_TYPE,
              disk_size=DEFAULT_DISK_SIZE,
              service_account=None,
              scopes=DEFAULT_SCOPE):
    if isinstance(scopes, str):
      scopes = [scopes]
    return super(ResourcesConfig, cls).__new__(
        cls, project, region, machine_type, disk_size, service_account, scopes)


class JobParams(object):
  """Container for job parameters."""

  def __init__(self, envs, inputs, r_inputs, outputs, r_outputs):
    self._check_for_collisions([envs, inputs, r_inputs, outputs, r_outputs])
    self.envs = envs
    self.inputs = inputs
    self.recursive_inputs = r_inputs
    self.outputs = outputs
    self.recursive_outputs = r_outputs

  def _check_for_collisions(self, allinputs):
    """Takes a list of iterable containers."""
    known_names = set()
    duplicates = []
    for vset in allinputs:
      for v in vset:
        if v.name not in known_names:
          known_names.add(v.name)
        else:
          duplicates.append(v.name)
    if duplicates:
      raise ValueError('Bad job config; duplicate names found: %r' % duplicates)

  def values(self):
    """Return each contained job parameter iterable."""
    return [self.envs,
            self.inputs,
            self.recursive_inputs,
            self.outputs,
            self.recursive_outputs]
