import urllib
import sys

from cinder.openstack.common import log as logging
from cinder.volume import driver
from cinder import exception

sys.path.insert(0, '/usr/share/stratostorage/mancala_management_api.egg')
from mancala.management.api import api
from mancala.management.externalapi import volumes
from mancala.management.externalapi import snapshots
from mancala.management.externalapi import images

LOG = logging.getLogger(__name__)
GB = float( 1073741824 )

class MancalaDriver(driver.VolumeDriver):
    """Implements Mancala (StratoStorage) volume commands."""

    VERSION = '1.0.0'

    def __init__(self, *args, **kwargs):
        super(MancalaDriver, self).__init__(*args, **kwargs)
        self._volume_api = volumes.VolumeAPI()
        self._snapshot_api = snapshots.SnapshotAPI()
        self._image_api = images.ImageAPI()
        self._mancala_api = api.API()
        self._gotUpdate = False
        self._stats = {}

    def do_setup(self, context):
        """Any initialization the volume driver does while starting"""
        pass

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""
        pass

    def create_volume(self, volume):
        """Creates a logical volume."""
        vol = self._volume_api.create(int(volume['size']), tag=str(volume['id']))
        return {'provider_location': vol['externalID']}

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        srcExternalID = self._extract_mancala_id(snapshot)
        vol = self._volume_api.createFrom(srcExternalID, tag=str(volume['id']))
        return {'provider_location': vol['externalID']}

    def _parse_location(self, location):
        prefix = 'mancala://'
        if not location.startswith(prefix):
            reason = _('Not stored in mancala')
            raise exception.ImageUnacceptable(image_id=location, reason=reason)
        pieces = map(urllib.unquote, location[len(prefix):].split('/'))
        if any(map(lambda p: p == '', pieces)):
            reason = _('Blank components')
            raise exception.ImageUnacceptable(image_id=location, reason=reason)
        if len(pieces) != 1:
            reason = _('Invalid URL')
            raise exception.ImageUnacceptable(image_id=location, reason=reason)
        return str(pieces[0])

    def clone_image(self, volume, image_location, image_id, image_meta):
        image_location = image_location[0] if image_location else None
        if image_location is None:
            return ({}, False)
        try:
            image_id = self._parse_location(image_location)
        except exception.ImageUnacceptable as e:
            LOG.debug(_('Not cloneable: %s'), e)
            return False
        LOG.info( 'Clone image %s' % image_id )
        vol = self._volume_api.createFrom(image_id, tag=str(volume['id']))
        return {'provider_location': vol['externalID']}, True

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        volExternalID = self._extract_mancala_id(volume)
        vol = self._image_api.createFrom(volExternalID, tag=str(image_meta['id']))
        uri = 'mancala://%s' % vol['externalID']
        image_service.update(context, image_meta['id'], {'location': uri })

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        srcExternalID = self._extract_mancala_id(src_vref)
        vol = self._volume_api.createFrom(srcExternalID, tag=str(volume['id']))
        return {'provider_location': vol['externalID']}

    def delete_volume(self, volume):
        """Deletes a volume."""
        self._volume_api.delete(self._extract_mancala_id(volume))

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        srcExternalID = self._extract_mancala_id(snapshot['volume'])
        vol = self._snapshot_api.createFrom(srcExternalID, tag=str(snapshot['id']))
        return {'provider_location': vol['externalID']}

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        self._snapshot_api.delete(self._extract_mancala_id(snapshot))

    def ensure_export(self, context, volume):
        """Synchronously recreates an export for a logical volume."""
        pass

    def create_export(self, context, volume):
        """Exports the volume."""
        pass

    def remove_export(self, context, volume):
        """Removes an export for a logical volume."""
        pass

    def initialize_connection(self, volume, connector):
        vol_info = self._volume_api.attach(self._extract_mancala_id(volume), str(connector['host']))
        name = '%(dynid)s:%(lunid)s:%(size)s' % {'dynid': vol_info['dynastyID'],
                                                 'lunid': vol_info['lunID'],
                                                 'size': vol_info['size']}
        data = {
            'driver_volume_type': 'mancala',
            'data': {'name': name}
        }
        LOG.debug('connection data: %s', data)
        return data

    def terminate_connection(self, volume, connector, **kwargs):
        self._volume_api.detach(self._extract_mancala_id(volume), str(connector['host']))

    def extend_volume(self, volume, new_size):
        self._volume_api.extend(self._extract_mancala_id(volume), int(new_size))

    def _extract_mancala_id(self, volume):
        return str(volume['provider_location'])

    def _update_volume_stats(self):
        mancala_stats = self._mancala_api.getBackendStats()
        if len(mancala_stats) < 2 and not self._gotUpdate:
            total = 'unknown'
            free = 'unknown'
        else:
            total = (mancala_stats['avail'] + mancala_stats['used']) / GB
            free = mancala_stats['avail'] / GB
            self._gotUpdate = True
        stats = {
            'vendor_name': 'StratoScale',
            'driver_version': self.VERSION,
            'storage_protocol': 'mancala',
            'total_capacity_gb': total,
            'free_capacity_gb': free,
            'reserved_percentage': 0,
        }
        backend_name = self.configuration.safe_get('volume_backend_name')
        stats['volume_backend_name'] = backend_name or 'rack-storage'
        self._stats = stats

    def get_volume_stats(self, refresh=False):
        """Return the current state of the volume service.

        If 'refresh' is True, run the update first.
        """
        if refresh:
            self._update_volume_stats()
        return self._stats
