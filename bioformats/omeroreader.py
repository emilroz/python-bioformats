# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------
  Copyright (C) 2016 Glencoe Software, Inc. All rights reserved.


  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

------------------------------------------------------------------------------
"""


from omero.gateway import BlitzGateway
import numpy as np
import re
import logging
logger = logging.getLogger(__name__)


class OmeroImageReader(object):

    REGEX_INDEX_FROM_FILE_NAME = r'[^\d-]+'

    def __init__(self, url, host, session_id):
        '''
        '''
        self.url = url
        self.host = host
        self.session_id = session_id
        self.gateway = None
        self.image = None
        self.metadata = None
        self.extract_id = re.compile(self.REGEX_INDEX_FROM_FILE_NAME)

    def __enter__(self):
        '''
        '''
        return self

    def __exit__(self):
        '''
        '''
        self.close()

    def close(self):
        '''
        '''
        if self.gateway is not None:
            self.gateway.seppuku(softclose=True)

    def init_reader(self):
        '''
        '''
        self.gateway = BlitzGateway(host=self.host)
        connected = False
        try:
            connected = self.gateway.connect(sUuid=self.session_id)
        except:
            message = "Couldn't connect to OMERO server"
            logger.exception(message)
            raise Exception(message)
        if not connected:
            message = "Couldn't connect to OMERO server"
            logger.exception(message)
            raise Exception(message)
        image_id = int(self.extract_id.sub('', self.url))
        try:
            self.image = self.gateway.getObject("Image", image_id)
        except:
            message = "Image Id: %s not found on the server." % image_id
            logger.error(message)
            raise Exception(message)

    def read(self, c=None, z=0, t=0, series=None, index=None,
             rescale=True, wants_max_intensity=False, channel_names=None,
             XYWH=None):
        '''
        '''
        if c is None and index is not None:
            c = index
        debug_message = \
            "Reading C: %s, Z: %s, T: %s, series: %s, index: %s, " \
            "channel names: %s, rescale: %s, wants_max_intensity: %s, " \
            "XYWH: %s" % (c, z, t, series, index, channel_names, rescale,
                          wants_max_intensity, XYWH)
        logger.debug(debug_message)
        if self.gateway is None:
            self.init_reader()
        message = None
        if t >= self.image.getSizeT():
            message = "T index %s exceeds sizeT %s" % \
                      (t, self.image.getSizeT())
            logger.error(message)
        if c >= self.image.getSizeC():
            message = "C index %s exceeds sizeC %s" % \
                      (c, self.image.getSizeC())
            logger.error(message)
        if z >= self.image.getSizeZ():
            message = "Z index %s exceeds sizeZ %s" % \
                      (z, self.image.getSizeZ())
            logger.error(message)
        if message is not None:
            raise Exception("Couldn't retrieve a plane from OMERO image.")
        pixels = self.image.getPrimaryPixels()
        image = None
        if c is None:
            planes = []
            for channel in range(self.image.getSizeC()):
                planes.append(pixels.getPlane(z, channel, t))
            image = np.dstack(planes)
        else:
            image = pixels.getPlane(z, c, t)
        if wants_max_intensity:
            return image, 1
        return image
