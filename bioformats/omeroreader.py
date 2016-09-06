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


import omero.clients
import numpy
from struct import unpack
import re
import logging
logger = logging.getLogger(__name__)


'''
The rescaling is probably super broken for Signed Types. Need to further
investigate why only single value for scale is used.
For the implementation that's mirrored in here look at:
    https://github.com/CellProfiler/python-bioformats/blob/master/bioformats/formatreader.py#L767
'''


def pixelRange(byte_width, signed):
    max_value = 2 ** (8 * byte_width)
    if signed:
        return (- (max_value / 2), (max_value / 2) - 1)
    else:
        return (0, max_value - 1)


class OmeroImageReader(object):

    REGEX_INDEX_FROM_FILE_NAME = r'[^\d-]+'
    PIXEL_TYPES = {
        "int8":   ['b', numpy.int8,    pixelRange(1, True)],
        "uint8":  ['B', numpy.uint8,   pixelRange(1, False)],
        "int16":  ['h', numpy.int16,   pixelRange(2, True)],
        "uint16": ['H', numpy.uint16,  pixelRange(2, False)],
        "int32":  ['i', numpy.int32,   pixelRange(4, True)],
        "uint32": ['I', numpy.uint32,  pixelRange(4, False)],
        "float":  ['f', numpy.float32, (0, 1)],
        "double": ['d', numpy.float64, (0, 1)]}
    SCALE_ONE_TYPE = ["float", "double"]

    def __init__(self, url, host, session_id):
        '''
        Initalise the reader by passing url in 'omero::idd=image_id' format.
        host = omero server address.
        session_id = session to join.
        '''
        logger.debug("Initializing OmeroPy reader for %s" % url)
        self.url = url
        self.host = host
        self.session_id = session_id
        # Connection setup
        self.client = None
        self.session = None
        # Omero services
        self.container_service = None
        # Omero objects
        self.omero_image = None
        self.pixels = None
        # Image info
        self.width = None
        self.height = None
        self.metadata = None
        self.extract_id = re.compile(self.REGEX_INDEX_FROM_FILE_NAME)
        self.path = url  # This guy is needed for reader caching

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
        Close connection to the server.
        Important step. Closes all the services on the server freeing up
        the resources.
        '''
        logger.debug("Closing OmeroPyReader")
        if self.client is not None:
            self.client.closeSession()

    def init_reader(self):
        '''
        Connect to OMERO server by joining session id.
        Request the OMERO.image from the server.
        Regex converts "omero::iid=image_id" to image_id.

        After reader is initaillised images can be read from the server.

        Connection to the server is terminated on close call.
        '''
        logger.debug("Initializing OmeroPyReader")
        if self.client is not None:
            return
        self.client = omero.client(self.host)
        connected = False
        try:
            self.session = self.client.joinSession(self.session_id)
            self.container_service = self.session.getContainerService()
        except:
            message = "Couldn't connect to OMERO server"
            logger.exception(message, exc_info=True)
            raise Exception(message)
        image_id = int(self.extract_id.sub('', self.url))
        try:
            self.omero_image = self.container_service.getImages(
                "Image", [image_id], None)[0]
        except:
            message = "Image Id: %s not found on the server." % image_id
            logger.error(message)
            raise Exception(message)
        self.pixels = self.omero_image.getPrimaryPixels()
        self.width = self.pixels.getSizeX().val
        self.height = self.pixels.getSizeY().val

    def read_planes(self, z=0, c=None, t=0, tile=None):
        channels = []
        if c is None:
            channels = range(self.pixels.getSizeC().val)
        else:
            channels.append(c)
        pixel_type = self.pixels.getPixelsType().value.val
        numpy_type = self.PIXEL_TYPES[pixel_type][1]
        raw_pixels_store = self.session.createRawPixelsStore()
        try:
            raw_pixels_store.setPixelsId(self.pixels.getId().val, True, None)
            logger.debug("Reading pixels Id: %s" % self.pixels.getId().val)
            planes = []
            for channel in channels:
                if tile is None:
                    sizeX = self.width
                    sizeY = self.height
                    raw_plane = raw_pixels_store.getPlane(z, c, t)
                else:
                    x, y, sizeX, sizeY = tile
                    raw_plane = raw_pixels_store.getTile(
                        z, c, t, x, y, sizeX, sizeY)
                convert_type = '>%d%s' % (
                    (sizeY * sizeX), self.PIXEL_TYPES[pixel_type][0])
                converted_plane = unpack(convert_type, raw_plane)
                plane = numpy.array(converted_plane, numpy_type)
                plane.resize(sizeY, sizeX)
                planes.append(plane)
            if c is None:
                return numpy.dstack(planes)
            else:
                return planes[0]
        except Exception:
            logger.error("Failed to get plane from OMERO", exc_info=True)
        finally:
            raw_pixels_store.close()

    def read(self, c=None, z=0, t=0, series=None, index=None,
             rescale=True, wants_max_intensity=False, channel_names=None,
             XYWH=None):
        '''
        Read a single plane from the image reader file.
        :param c: read from this channel. `None` = read color image if
            multichannel or interleaved RGB.
        :param z: z-stack index
        :param t: time index
        :param series: series for ``.flex`` and similar multi-stack formats
        :param index: if `None`, fall back to ``zct``, otherwise load the
            indexed frame
        :param rescale: `True` to rescale the intensity scale to 0 and 1;
            `False` to return the raw values native to the file.
        :param wants_max_intensity: if `False`, only return the image;
            if `True`, return a tuple of image and max intensity
        :param channel_names: provide the channel names for the OME metadata
        :param XYWH: a (x, y, w, h) tuple
        '''
        if c is None and index is not None:
            c = index
        debug_message = \
            "Reading C: %s, Z: %s, T: %s, series: %s, index: %s, " \
            "channel names: %s, rescale: %s, wants_max_intensity: %s, " \
            "XYWH: %s" % (c, z, t, series, index, channel_names, rescale,
                          wants_max_intensity, XYWH)
        logger.debug(debug_message)
        if self.session is None:
            self.init_reader()
        message = None
        if t >= self.pixels.getSizeT().val:
            message = "T index %s exceeds sizeT %s" % \
                      (t, self.pixels.getSizeT().val)
            logger.error(message)
        if c >= self.pixels.getSizeC().val:
            message = "C index %s exceeds sizeC %s" % \
                      (c, self.pixels.getSizeC().val)
            logger.error(message)
        if z >= self.pixels.getSizeZ().val:
            message = "Z index %s exceeds sizeZ %s" % \
                      (z, self.pixels.getSizeZ().val)
            logger.error(message)
        if message is not None:
            raise Exception("Couldn't retrieve a plane from OMERO image.")
        tile = None
        if XYWH is not None:
            assert isinstance(XYWH, tuple) and len(XYWH) == 4, \
                "Invalid XYWH tuple"
            tile = XYWH
        numpy_image = self.read_planes(z, c, t, tile)
        pixel_type = self.pixels.getPixelsType().value.val
        min_value = self.PIXEL_TYPES[pixel_type][2][0]
        max_value = self.PIXEL_TYPES[pixel_type][2][1]
        logger.debug("Pixel range [%s, %s]" % (min_value, max_value))
        if rescale:
            logger.debug("Rescaling image by %s" % max_value)
            numpy_image = \
                numpy_image.astype(numpy.float32) / float(max_value)
        if wants_max_intensity:
            return numpy_image, max_value
        return numpy_image
