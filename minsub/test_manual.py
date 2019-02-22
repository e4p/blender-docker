#!/usr/bin/env python3
"""Test manually, delete this eventually.

Lint code with SublimeLinter and SublimeLinter-flake8.
Ensure flake8 is installed in the system python3 library
by using sudo pip3
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import code
import param_util
import pipeline_actions
import pipeline_api
import model


def main():
  job = param_util.args_to_job_params(
      envs=["A='this is my string'", 'VAR2=yeppers peppers'],
      inputs=['F1=gs://mybucket/myfile.txt'],
      inputs_recur=['F2=gs://mybucket/myfilecollection/here/'],
      outputs=['FO1=gs://mybucket/myoutfilefile.txt'],
      outputs_recur=['FO2=gs://mybucket/myfilecollection/here/'],
  )
  res = model.ResourcesConfig('dros-helper', 'us-west1')
  cmd = ['/bin/bash', '-c', 'echo "${A}" && echo "${F1}"']
  actions = [
      pipeline_actions.LocalizeAction(job),
      pipeline_actions.UserAction(job, 'evan-run', 'debian:stable-slim', cmd),
      pipeline_actions.DelocalizeAction(job),
  ]
  plr = pipeline_api.create_pipeline_request(res, job, actions)

  code.interact(local=locals())


if __name__ == '__main__':
  main()
