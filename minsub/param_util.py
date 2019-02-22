# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility functions and classes for dsub command-line parameters."""

from __future__ import absolute_import
from __future__ import print_function

import collections
import os
import re

import model


AUTO_PREFIX_INPUT = 'INPUT_'  # Prefix for auto-generated input names
AUTO_PREFIX_OUTPUT = 'OUTPUT_'  # Prefix for auto-generated output names

DEFAULT_INPUT_LOCAL_PATH = 'input'
DEFAULT_OUTPUT_LOCAL_PATH = 'output'


class FileParam(
    collections.namedtuple('FileParam', [
        'name', 'value', 'docker_path', 'uri', 'recursive',
    ])):
  """File parameter to be automatically localized or de-localized.

  Input files are automatically localized to the pipeline VM's local disk.

  Output files are automatically de-localized to a remote URI from the
  pipeline VM's local disk.

  Attributes:
    name (str): the parameter and environment variable name.
    value (str): the original value given by the user on the command line or
                 in the TSV file.
    docker_path (str): the on-VM location; also set as the environment variable
                       value.
    uri (UriParts): A uri or local file path.
    recursive (bool): Whether recursive copy is wanted.
  """
  __slots__ = ()

  def __new__(cls, name, value, docker_path, uri, recursive):
    return super(FileParam, cls).__new__(cls, name, value, docker_path, uri,
                                         recursive)


class InputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False):
    _validate_param_name(name, 'Input parameter')
    return super(InputFileParam, cls).__new__(
        cls, name, value, docker_path, uri, recursive)


class OutputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False):
    _validate_param_name(name, 'Output parameter')
    return super(OutputFileParam, cls).__new__(
        cls, name, value, docker_path, uri, recursive)


class UriParts(str):
  """Subclass string for multipart URIs.

  This string subclass is used for URI references. The path and basename
  attributes are used to maintain separation of this information in cases where
  it might otherwise be ambiguous. The value of a UriParts string is a URI.

  Attributes:
    path: Strictly speaking, the path attribute is the entire leading part of
      a URI (including scheme, host, and path). This attribute defines the
      hierarchical location of a resource. Path must end in a forward
      slash. Local file URIs are represented as relative URIs (path only).
    basename: The last token of a path that follows a forward slash. Generally
      this defines a specific resource or a pattern that matches resources. In
      the case of URI's that consist only of a path, this will be empty.

  Examples:
    | uri                         |  uri.path              | uri.basename  |
    +-----------------------------+------------------------+---------------|
    | gs://bucket/folder/file.txt | 'gs://bucket/folder/'  | 'file.txt'    |
    | http://example.com/1.htm    | 'http://example.com/'  | '1.htm'       |
    | /tmp/tempdir1/              | '/tmp/tempdir1/'       | ''            |
    | /tmp/ab.txt                 | '/tmp/'                | 'ab.txt'      |
  """

  def __new__(cls, path, basename):
    basename = basename if basename is not None else ''
    newuri = str.__new__(cls, path + basename)
    newuri.path = path
    newuri.basename = basename
    return newuri


class EnvParam(collections.namedtuple('EnvParam', ['name', 'value'])):
  """Name/value input parameter to a pipeline.

  Attributes:
    name (str): the input parameter and environment variable name.
    value (str): the variable value (optional).
  """
  __slots__ = ()

  def __new__(cls, name, value=None):
    _validate_param_name(name, 'Environment variable')
    return super(EnvParam, cls).__new__(cls, name, value)


class FileParamUtil(object):
  """Base class helper for producing FileParams from args or a tasks file.

  InputFileParams and OutputFileParams can be produced from either arguments
  passed on the command-line or as a combination of the definition in the tasks
  file header plus cell values in task records.

  This class encapsulates the generation of the FileParam name, if none is
  specified (get_variable_name()) as well as common path validation for
  input and output arguments (validate_paths).
  """

  def __init__(self, auto_prefix, relative_path):
    self.param_class = FileParam
    self._auto_prefix = auto_prefix
    self._auto_index = 0
    self._relative_path = relative_path

  def get_variable_name(self, name):
    """Produce a default variable name if none is specified."""
    if not name:
      name = '%s%s' % (self._auto_prefix, self._auto_index)
      self._auto_index += 1
    return name

  @staticmethod
  def _validate_file_provider(uri):
    """Find the file provider for a URI."""
    if uri.startswith('gs://'):
      return
    else:
      raise ValueError('Expected GCS location found: %s' % uri)

  @staticmethod
  def _validate_paths_or_fail(uri, recursive):
    """Do basic validation of the uri, return the path and filename."""
    path, filename = os.path.split(uri)

    # minsub could support character ranges ([0-9]) with some more work, but for
    # now we assume that basic asterisk wildcards are sufficient. Reject any URI
    # that includes square brackets or question marks, since we know that
    # if they actually worked, it would be accidental.
    if '[' in uri or ']' in uri:
      raise ValueError(
          'Square bracket (character ranges) are not supported: %s' % uri)
    if '?' in uri:
      raise ValueError('Question mark wildcards are not supported: %s' % uri)

    # Only support file URIs and *filename* wildcards
    # Wildcards at the directory level or "**" syntax would require better
    # support from the Pipelines API *or* doing expansion here and
    # (potentially) producing a series of FileParams, instead of one.
    if '*' in path:
      raise ValueError(
          'Path wildcard (*) are only supported for files: %s' % uri)
    if '**' in filename:
      raise ValueError('Recursive wildcards ("**") not supported: %s' % uri)
    if filename in ('..', '.'):
      raise ValueError('Path characters ".." and "." not supported '
                       'for file names: %s' % uri)

    # Do not allow non-recursive IO to reference directories.
    if not recursive and not filename:
      raise ValueError('Input or output values that are not recursive must '
                       'reference a filename or wildcard: %s' % uri)

  def parse_uri(self, raw_uri, recursive):
    """Return a valid docker_path and uri from a GCS flag value."""
    # Assume recursive URIs are directory paths.
    if recursive:
      raw_uri = directory_fmt(raw_uri)
    # Validate the file provider & raw URI, then rewrite the path
    # component of the URI for docker and remote.
    self._validate_file_provider(raw_uri)
    self._validate_paths_or_fail(raw_uri, recursive)
    uri, docker_uri = _gcs_uri_rewriter(raw_uri)
    uri_parts = UriParts(
        directory_fmt(os.path.dirname(uri)), os.path.basename(uri))
    return docker_uri, uri_parts

  def make_param(self, name, raw_uri, recursive):
    """Return a *FileParam given an input uri."""
    if not raw_uri:
      return self.param_class(name, None, None, None, recursive)
    docker_path, uri_parts = self.parse_uri(raw_uri, recursive)
    return self.param_class(name, raw_uri, docker_path, uri_parts, recursive)


class InputFileParamUtil(FileParamUtil):
  """Implementation of FileParamUtil for input files."""

  def __init__(self, docker_path):
    super(InputFileParamUtil, self).__init__(AUTO_PREFIX_INPUT, docker_path)
    self.param_class = InputFileParam


class OutputFileParamUtil(FileParamUtil):
  """Implementation of FileParamUtil for output files."""

  def __init__(self, docker_path):
    super(OutputFileParamUtil, self).__init__(AUTO_PREFIX_OUTPUT, docker_path)
    self.param_class = OutputFileParam


def _gcs_uri_rewriter(raw_uri):
  """Rewrite GCS file path to a docker mount.

  The GCS rewriter performs no operations on the raw_path and simply returns
  it as the normalized URI. The docker path has the gs:// prefix replaced
  with gs/ so that it can be mounted inside a docker image.

  Args:
    raw_uri: (str) the raw GCS URI, prefix, or pattern.

  Returns:
    normalized: a cleaned version of the uri provided by command line.
    docker_path: the uri rewritten in the format required for mounting inside
                 a docker worker.
  """
  docker_path = raw_uri.replace('gs://', 'gs/', 1)
  return raw_uri, docker_path


def _local_uri_rewriter(raw_uri):
  """Rewrite local file URIs as required by the rewrite_uris method.

  Local file paths, unlike GCS paths, may have their raw URI simplified by
  os.path.normpath which collapses extraneous indirect characters.

  >>> _local_uri_rewriter('/tmp/a_path/../B_PATH/file.txt')
  ('/tmp/B_PATH/file.txt', 'file/tmp/B_PATH/file.txt')
  >>> _local_uri_rewriter('/myhome/./mydir/')
  ('/myhome/mydir/', 'file/myhome/mydir/')

  The local path rewriter will also work to preserve relative paths even
  when creating the docker path. This prevents leaking of information on the
  invoker's system to the remote system. Doing this requires a number of path
  substitutions denoted with the _<rewrite>_ convention.

  >>> _local_uri_rewriter('./../upper_dir/')[1]
  'file/_dotdot_/upper_dir/'
  >>> _local_uri_rewriter('~/localdata/*.bam')[1]
  'file/_home_/localdata/*.bam'

  Args:
    raw_uri: (str) the raw file or directory path.

  Returns:
    normalized: a simplified and/or expanded version of the uri.
    docker_path: the uri rewritten in the format required for mounting inside
                 a docker worker.

  """
  # The path is split into components so that the filename is not rewritten.
  raw_path, filename = os.path.split(raw_uri)
  # Generate the local path that can be resolved by filesystem operations,
  # this removes special shell characters, condenses indirects and replaces
  # any unnecessary prefix.
  prefix_replacements = [('file:///', '/'), ('~/', os.getenv('HOME')), ('./',
                                                                        ''),
                         ('file:/', '/')]
  normed_path = raw_path
  for prefix, replacement in prefix_replacements:
    if normed_path.startswith(prefix):
      normed_path = os.path.join(replacement, normed_path[len(prefix):])
  # Because abspath strips the trailing '/' from bare directory references
  # other than root, this ensures that all directory references end with '/'.
  normed_uri = directory_fmt(os.path.abspath(normed_path))
  normed_uri = os.path.join(normed_uri, filename)

  # Generate the path used inside the docker image;
  #  1) Get rid of extra indirects: /this/./that -> /this/that
  #  2) Rewrite required indirects as synthetic characters.
  #  3) Strip relative or absolute path leading character.
  #  4) Add 'file/' prefix.
  docker_rewrites = [(r'/\.\.', '/_dotdot_'), (r'^\.\.', '_dotdot_'),
                     (r'^~/', '_home_/'), (r'^file:/', '')]
  docker_path = os.path.normpath(raw_path)
  for pattern, replacement in docker_rewrites:
    docker_path = re.sub(pattern, replacement, docker_path)
  docker_path = docker_path.lstrip('./')  # Strips any of '.' './' '/'.
  docker_path = directory_fmt('file/' + docker_path) + filename
  return normed_uri, docker_path


def split_pair(pair_string, separator, nullable_idx=1):
  """Split a string into a pair, which can have one empty value.

  Args:
    pair_string: The string to be split.
    separator: The separator to be used for splitting.
    nullable_idx: The location to be set to null if the separator is not in the
                  input string. Should be either 0 or 1.

  Returns:
    A list containing the pair.

  Raises:
    IndexError: If nullable_idx is not 0 or 1.
  """

  pair = pair_string.split(separator, 1)
  if len(pair) == 1:
    if nullable_idx == 0:
      return [None, pair[0]]
    elif nullable_idx == 1:
      return [pair[0], None]
    else:
      raise IndexError('nullable_idx should be either 0 or 1.')
  else:
    return pair


def parse_pair_args(labels, argclass):
  """Parse flags of key=value pairs and return a list of argclass.

  For pair variables, we need to:
     * split the input into name=value pairs (value optional)
     * Create the EnvParam object

  Args:
    labels: list of 'key' or 'key=value' strings.
    argclass: Container class for args, must instantiate with argclass(k, v).

  Returns:
    list of argclass objects.
  """
  label_data = set()
  for arg in labels:
    name, value = split_pair(arg, '=', nullable_idx=1)
    label_data.add(argclass(name, value))
  return label_data


def args_to_job_params(envs, inputs, inputs_recur, outputs, outputs_recur):
  """Parse env, input, and output parameters into a job parameters and data.

  Passing arguments on the command-line allows for launching a single job.
  The env, input, and output arguments encode both the definition of the
  job as well as the single job's values.

  Env arguments are simple name=value pairs.
  Input and output file arguments can contain name=value pairs or just values.
  Either of the following is valid:

    uri
    myfile=uri

  Args:
    envs: list of environment variable job parameters
    inputs: list of file input parameters
    inputs_recur: list of recursive directory input parameters
    outputs: list of file output parameters
    outputs_recur: list of recursive directory output parameters

  Returns:
    job_params: a dictionary of 'envs', 'inputs', and 'outputs' that defines the
    set of parameters and data for a job.
  """
  input_file_param_util = InputFileParamUtil(DEFAULT_INPUT_LOCAL_PATH)
  output_file_param_util = OutputFileParamUtil(DEFAULT_OUTPUT_LOCAL_PATH)
  # Parse environmental variables and labels.
  env_data = parse_pair_args(envs, EnvParam)

  # For input files, we need to:
  #   * split the input into name=uri pairs (name optional)
  #   * get the environmental variable name, or automatically set if null.
  #   * create the input file param
  input_data, r_input_data = set(), set()
  for (r, sink, args) in ((False, input_data, inputs), (True, r_input_data, inputs_recur)):  # pylint: disable=line-too-long
    for arg in args:
      name, value = split_pair(arg, '=', nullable_idx=0)
      name = input_file_param_util.get_variable_name(name)
      sink.add(input_file_param_util.make_param(name, value, recursive=r))

  # For output files, we need to:
  #   * split the input into name=uri pairs (name optional)
  #   * get the environmental variable name, or automatically set if null.
  #   * create the output file param
  output_data, r_output_data = set(), set()
  for (r, sink, args) in ((False, output_data, outputs), (True, r_output_data, outputs_recur)):  # pylint: disable=line-too-long
    for arg in args:
      name, value = split_pair(arg, '=', 0)
      name = output_file_param_util.get_variable_name(name)
      sink.add(output_file_param_util.make_param(name, value, recursive=r))

  return model.JobParams(
      env_data,
      input_data,
      r_input_data,
      output_data,
      r_output_data,
  )


def directory_fmt(directory):
  """In ensure that directories end with '/'; fixes recursive copy."""
  return directory.rstrip('/') + '/'


def _validate_param_name(name, param_type):
  """Validate that the name follows posix conventions for env variables."""
  # http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_235
  #
  # 3.235 Name
  # In the shell command language, a word consisting solely of underscores,
  # digits, and alphabetics from the portable character set.
  if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
    raise ValueError('Invalid %s: %s' % (param_type, name))

