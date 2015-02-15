# Copyright 2011 Justin Santa Barbara
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""The backends api."""

from cinder.api.openstack import wsgi
from cinder import db
from cinder.scheduler import rpcapi as scheduler_rpcapi


class BackendController(wsgi.Controller):
    """The Backends API controller for the OpenStack API."""

    _visible_admin_metadata_keys = ['readonly', 'attached_mode']

    def __init__(self, ext_mgr):
        self.scheduler_rpcapi = scheduler_rpcapi.SchedulerAPI()
        self.ext_mgr = ext_mgr
        super(BackendController, self).__init__()

    def detail(self, req, backend_id=None):
        name_opt = req.GET.copy()
        name = name_opt.pop('backend_name', None)
        """Returns memory statistics of backends"""
        context = req.environ['cinder.context']
        backends_data = self.scheduler_rpcapi.get_backend_data(context, 'cinder-scheduler')

        backends_final_data = self._extract_backend_by_name(backends_data, name)
        for backend in backends_final_data:
            (count, sum) = db.volume_data_get_for_host(context, backend)
            backends_final_data[backend]['volume_count'] = str(count)
            backends_final_data[backend]['total_volume_gb'] = str(sum)
            (count, sum) = db.snapshot_data_get_for_host(context, backend)
            backends_final_data[backend]['snapshot_count'] = str(count)
            backends_final_data[backend]['total_snapshot_gb'] = str(sum)

        return {'backends': backends_final_data}

    def _extract_backend_by_name(self, backends_dict, name):
        if name is None:
            return backends_dict

        new_backend = {}
        for k, backend in backends_dict.iteritems():
            if backend['volume_backend_name'] == name:
                new_backend[k] = backend
                break
        return new_backend

def create_resource(ext_mgr):
    return wsgi.Resource(BackendController(ext_mgr))


