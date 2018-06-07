#!/usr/bin/env python3

import isce
from isceobj.Scene.Frame import Frame
from isceobj.Planet.AstronomicalHandbook import Const
from isceobj.Planet.Planet import Planet

from Sentinel1_TOPS import Sentinel1_TOPS
import argparse
import os, re
from lxml import objectify as OBJ
from FrameInfoExtractor import FrameInfoExtractor as FIE


DATASETTYPE_RE = re.compile(r'-(raw|slc)-')

MISSION_RE = re.compile(r'S1(\w)')


def cmdLineParse():
    '''
    Command line parsing.
    '''

    parser = argparse.ArgumentParser(description='Extract metadata from S1 swath')
    #parser.add_argument('-i','--input', dest='inxml', type=str, required=True,
            #help='Swath XML file')a
    parser.add_argument('-i','--input', dest='xml_file', type=str, nargs='+', help='Swath XML file')
    parser.add_argument('-o', '--output', dest='outjson', type=str, required=True,
            help = 'Ouput met.json')
    return parser.parse_args()

def objectify(inxml):
    '''
    Return objectified XML.
    '''
    with open(inxml, 'r') as fid:
        root = OBJ.parse(fid).getroot()
    return root

def getGeometry(obj):
    '''
    Get bbox and central coordinates.
    '''
    pts = []
    glist = obj.geolocationGrid.geolocationGridPointList

    for child in glist.getchildren():
        pts.append( [float(child.line), float(child.pixel), float(child.latitude), float(child.longitude)])

    ys = sorted(list(set([x[0] for x in pts])))
    dy = ys[1] - ys[0]
    ny= int((ys[-1] - ys[0])/dy + 1)

    xs = sorted(list(set([x[1] for x in pts])))
    dx = xs[1] - xs[0]
    nx = int((xs[-1] - xs[0])/dx + 1)

    lat = np.array([x[2] for x in pts]).reshape((ny,nx))
    lon = np.array([x[3] for x in pts]).reshape((ny,nx))

    bbox = [[lat[0,0],lon[0,0]], [lat[0,-1],lon[0,-1]],
            [lat[-1,-1],lon[-1,-1]], [lat[-1,0], lon[-1,0]]]

    center = { "coordinates": [lon[ny//2,nx//2], lat[ny//2, nx//2]],
               "type" : "point"}

    return center, bbox


class S1toFrame(object):
    '''
    Create a traditional ISCE Frame object from S1 container.
    '''

    def __init__(self, sar, obj):
        self.sar = sar
        self.obj = obj
        self.missionId = self.obj.xpath('.//missionId/text()')[0]
        self.missionId_char = MISSION_RE.search(self.missionId).group(1)
        self.frame = Frame()
        self.frame.configure()

        self.parse()

    def parse(self):
        self._populatePlatform()
        self._populateInstrument()
        self._populateFrame()
        self._populateOrbit()
        self._populateExtras()

    def _populatePlatform(self):
        platform = self.frame.getInstrument().getPlatform()
        platform.setMission(self.missionId)
        platform.setPlanet(Planet(pname='Earth'))
        platform.setPointingDirection(-1)
        platform.setAntennaLength(40.0)
        
    def _populateInstrument(self):
        ins = self.frame.getInstrument()
        b0 = self.sar.bursts[0]
        b1 = self.sar.bursts[-1]

        ins.setRadarWavelength(b0.radarWavelength)
        ins.setPulseRepetitionFrequency(1.0/b0.azimuthTimeInterval)
        ins.setRangePixelSize(b0.rangePixelSize)

        tau = self.obj.generalAnnotation.replicaInformationList.replicaInformation.referenceReplica.timeDelay
        ins.setPulseLength(float(tau))
        slope = str(self.obj.generalAnnotation.replicaInformationList.replicaInformation.referenceReplica.phaseCoefficients).split()[2]
        ins.setChirpSlope(float(slope))
        
        fsamp = Const.c / (2.0 * b0.rangePixelSize)
        ins.setRangeSamplingRate(fsamp)

        ins.setInPhaseValue(127.5)
        ins.setQuadratureValue(127.5)
        ins.setBeamNumber(self.obj.adsHeader.swath)
       
    def _populateFrame(self):
        frame = self.frame
        b0 = self.sar.bursts[0]
        b1 = self.sar.bursts[-1]


        hdg = self.obj.generalAnnotation.productInformation.platformHeading
        if hdg < -90:
            frame.setPassDirection('Descending')
        else:
            frame.setPassDirection('Ascending')

        frame.setStartingRange(b0.startingRange)
        frame.setOrbitNumber(int(self.obj.adsHeader.absoluteOrbitNumber))
        frame.setProcessingFacility('Sentinel 1%s' % self.missionId_char)
        frame.setProcessingSoftwareVersion('IPF')
        frame.setPolarization(self.obj.adsHeader.polarisation)
        frame.setNumberOfSamples(int(self.obj.imageAnnotation.imageInformation.numberOfSamples))
        frame.setNumberOfLines(int(self.obj.imageAnnotation.imageInformation.numberOfLines))
        frame.setSensingStart(b0.sensingStart)
        frame.setSensingStop(b1.sensingStop)

        tmid = b0.sensingStart + 0.5 * (b1.sensingStop - b0.sensingStart)
        frame.setSensingMid(tmid)
        
        farRange = b0.startingRange + frame.getNumberOfSamples() * b0.rangePixelSize
        frame.setFarRange(farRange)

    def _populateOrbit(self):
        b0 = self.sar.bursts[0]
        self.frame.orbit = b0.orbit


    def _populateExtras(self):
        b0 = self.sar.bursts[0]
        self.frame._squintAngle = 0.0
        self.frame.doppler = b0.doppler._coeffs[0]
        match = DATASETTYPE_RE.search(self.sar.xml)
        if match: self.frame.datasetType = 'slc'
        else: self.frame.datasetType = ''

def create_stitched_met_json(  met_files, met_json_file):
    """Create HySDS met json file."""

    # build met
    met = {
        'product_type': 'interferogram',
        'master_scenes': [],
        'refbbox': [],
        'esd_threshold': [],
        'frameID': [],
        'temporal_span': [],
        'swath': [1, 2, 3],
        'trackNumber': [],
        'dataset_type': 'slc',
        'tile_layers': [ 'amplitude', 'displacement' ],
        'parallelBaseline': [],
        'url': [],
        'doppler': [],
        'slave_scenes': [],
        'orbit_type': [],
        'spacecraftName': [],
        'frameNumber': None,
        'reference': None,
        'bbox': bbox,
        'ogr_bbox': [[x, y] for y, x in bbox],
        'orbitNumber': [],
        'inputFile': '"sentinel.ini',
        'perpendicularBaseline': [],
        'orbitRepeat': [],
        'polarization': [],
        'scene_count': 1,
        'beamID': None,
        'sensor': [],
        'lookDirection': [],
        'platform': [],
        'startingRange': [],
        'frameName': [],
        'tiles': True,
        'beamMode': [],
        'imageCorners': [],
        'direction': [],
        'prf': [],
        'range_looks': [],
        'dem_type': None,
        'filter_strength': [],
	'azimuth_looks': [],
        "sha224sum": hashlib.sha224(str.encode(os.path.basename(met_json_file))).hexdigest(),
    }

    # collect values
    set_params = ('master_scenes', 'esd_threshold', 'frameID', 'swath', 'parallelBaseline',
                  'doppler', 'version', 'slave_scenes', 'orbit_type', 'spacecraftName',
                  'orbitNumber', 'perpendicularBaseline', 'orbitRepeat', 'polarization', 
                  'sensor', 'lookDirection', 'platform', 'startingRange',
                  'beamMode', 'direction', 'prf', 'azimuth_looks')
    single_params = ('temporal_span', 'trackNumber', 'dem_type')
    list_params = ('platform', 'swath', 'perpendicularBaseline', 'parallelBaseline', 'range_looks','filter_strength')
    mean_params = ('perpendicularBaseline', 'parallelBaseline')
    for i, met_file in enumerate(met_files):
        with open(met_file) as f:
            md = json.load(f)
        for param in set_params:
            #logger.info("param: {}".format(param))
            if isinstance(md[param], list):
                met[param].extend(md[param])
            else:
                met[param].append(md[param])
        if i == 0:
            for param in single_params:
                met[param] = md[param]
        met['scene_count'] += 1
    for param in set_params:
        tmp_met = list(set(met[param]))
        if param in list_params:
            met[param] = tmp_met
        else:
            met[param] = tmp_met[0] if len(tmp_met) == 1 else tmp_met
    for param in mean_params:
        met[param] = np.mean(met[param])

    # write out dataset json
    with open(met_json_file, 'w') as f:
        json.dump(met, f, indent=2)


if __name__ == '__main__':
    '''
    Main driver.
    '''
    
    #Parse command line
    inps = cmdLineParse()

    #Read in metadata
    xml_files=inps.xml_file
    frame_infos=[]
    i=0
    for inxml in xml_files:
        i=i+1
	sar = Sentinel1_TOPS()
        met_file= "test_met%s.json"%i
        sar.xml = inxml
	print("Extract Metadata : Processing %s" %inxml)
        sar.parse()
        obj = objectify(inxml)
    
        ####Copy into ISCE Frame
        frame = S1toFrame(sar,obj)

        ####Frameinfoextractor
        fie = FIE()
        frameInfo = fie.extractInfoFromFrame(frame.frame)
        frame_infos.append(frameInfo)
        frameInfo.dump(met_file)

    create_stitched_met_json(  frame_infos, inps.outjson)

