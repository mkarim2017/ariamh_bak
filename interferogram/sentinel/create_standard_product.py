import os, sys, re, requests, json, logging, traceback, argparse, copy, bisect
import hashlib
from itertools import product, chain
from datetime import datetime, timedelta
#from hysds.celery import app
from utils.UrlUtils import UrlUtils as UU
from fetchOrbitES import fetch


# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('enumerate_acquisations')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


RESORB_RE = re.compile(r'_RESORB_')

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

IFG_ID_TMPL = "S1-IFG_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"
RSP_ID_TMPL = "S1-SLCP_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}_s{}-{}-{}"

BASE_PATH = os.path.dirname(__file__)
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"

def query_grq( doc_id):
    """
    This function queries ES
    :param endpoint: the value specifies which ES endpoint to send query
     can be MOZART or GRQ
    :param doc_id: id of product or job
    :return: result from elasticsearch
    """
    es_url, es_index = None, None

    '''
    if endpoint == GRQ_ES_ENDPOINT:
        es_url = app.conf["GRQ_ES_URL"]
        es_index = "grq"
    if endpoint == MOZART_ES_ENDPOINT:
        es_url = app.conf['JOBS_ES_URL']
        es_index = "job_status-current"
    '''

    uu = UU()
    logger.info("rest_url: {}".format(uu.rest_url))
    logger.info("grq_index_prefix: {}".format(uu.grq_index_prefix))

    # get normalized rest url
    es_url = uu.rest_url[:-1] if uu.rest_url.endswith('/') else uu.rest_url
    es_index = uu.grq_index_prefix

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}} # add job status:
                ]
            }
        }
    }
    #print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']

def get_dem_type(slc_source):
    dem_type = "SRTM+v3"
    if slc_source['city'] is not None and len(slc_source['city'])>0:
	if slc_source['city'][0]['country_name'] is not None and slc_source['city'][0]['country_name'].lower() == "united states":
	    dem_type="Ned1"
    return dem_type

def print_list(l):
    for f in l:
	print("\n%s"%f)

def initiate_standard_product_job(context_file):
    # get context
    with open(context_file) as f:
        context = json.load(f)

    print(context)
    context = context["payload"]
    precise_orbit_only = True
    projects = []
    stitched_args = []
    ifg_ids = []
    master_zip_urls = []
    master_orbit_urls = []
    slave_zip_urls = []
    slave_orbit_urls = []
    swathnums = [1, 2, 3]
    bboxes = []
    auto_bboxes = []
    orbit_dict = {}
    dem_type = "SRTM+v3"
    master_orbit_number = None
    slave_orbit_number = None
    #bboxes.append(bbox)
    #auto_bboxes.append(auto_bbox)
    projects.append(context["project"])


    #master_slcs = context["master_slc"]
    #slave_slcs = context["slave_slcs"]
    
    master_slcs = ["acquisition-S1A_IW_SLC__1SDV_20180807T135955_20180807T140022_023141_02837E_DA79"]
    slave_slcs =["acquisition-S1A_IW_SLC__1SDV_20180714T140019_20180714T140046_022791_027880_AFD3", "acquisition-S1A_IW_SLC__1SDV_20180714T135954_20180714T140021_022791_027880_D224", "acquisition-S1A_IW_SLC__1SDV_20180714T135929_20180714T135956_022791_027880_9FCA"]

    #print(master_slcs)

    print("Processing Master")
    for slc_id in master_slcs:
  	result = query_grq(slc_id)[0]['_source']
	track = result['metadata']['trackNumber']
	master_orbit_number = result['metadata']['orbitNumber']
        zip_url = get_prod_url(result['urls'], result['metadata']['archive_filename'])
	orbit_url = get_orbit_url (slc_id, track)
        dem_type = get_dem_type(result)
        print("%s : %s : %s : %s : %s : %s" %(master_orbit_number, slc_id, track, zip_url, orbit_url, dem_type))
	master_zip_urls.append(zip_url)
	master_orbit_urls.append(orbit_url)
    

    print("Processing Slaves")
    for slc_id in slave_slcs:
	result = query_grq(slc_id)[0]['_source']
        track = result['metadata']['trackNumber']
	slave_orbit_number = result['metadata']['orbitNumber']
        zip_url = get_prod_url(result['urls'], result['metadata']['archive_filename'])
        orbit_url = get_orbit_url (slc_id, track)
        dem_type = get_dem_type(result)
        print("%s : %s : %s : %s : %s : %s" %(slave_orbit_number, slc_id, track, zip_url, orbit_url, dem_type))
        slave_zip_urls.append(zip_url)
        slave_orbit_urls.append(orbit_url)


    print("\n\n\n Master Zips:")
    print_list(master_zip_urls)
    print("\nSlave Zip:")
    print_list(slave_zip_urls)
    print("\nMaster Orbit:")
    print_list(master_orbit_urls)
    print("\nSlave Orbit:")
    print_list(slave_orbit_urls)

    print("\n\n")
    # get orbit type
    orbit_type = 'poeorb'
    for o in master_orbit_urls+ slave_orbit_urls:
	print(o)
	if RESORB_RE.search(o):
	    orbit_type = 'resorb'
     	    break

    print(orbit_type)
    if orbit_type == 'resorb':
	logger.info("Precise orbit required. Filtering job configured with restituted orbit.")
    #else:
	swathnums=[1,2,3]
	ifg_hash = hashlib.md5(json.dumps([
                                    IFG_ID_TMPL,
                                    master_zip_urls[-1],
                                    master_orbit_urls[-1],
                                    slave_zip_urls[-1],
                                    slave_orbit_urls[-1],
                                    #bboxes[-1],
                                    #auto_bboxes[-1],
                                    projects[-1],
                                    dem_type
                                ])).hexdigest()
	ifg_id = IFG_ID_TMPL.format('M', len(master_slcs), len(slave_slcs), track, master_orbit_number, slave_orbit_number, swathnums, orbit_type, ifg_hash[0:4])
        
    return context['project'], True, ifg_id, master_zip_urls, master_orbit_urls, slave_zip_urls, slave_orbit_urls, context['bbox'], context['wuid'], context['job_num']


def get_prod_url (urls, archive_file):
    prod_url = urls[0]
    if len(urls) > 1:
     	for u in urls:
      	    if u.startswith('s3://'):
                prod_url = u
                break
        #print("prod_url : %s" %prod_url)
    zip_url = "%s/%s" % (prod_url, archive_file)
    return zip_url

def get_orbit_url (slc_id, track):
    orbit_url = None
    orbit_dict = {}

    try:

        match = SLC_RE.search(slc_id)
        #print("match : %s" %match)
        if not match:
            raise RuntimeError("Failed to recognize SLC ID %s." % h['_id'])
        slc_start_dt = datetime(int(match.group('start_year')),
                                int(match.group('start_month')),
                                int(match.group('start_day')),
                                int(match.group('start_hour')),
                                int(match.group('start_min')),
                                int(match.group('start_sec')))
        #print("slc_start_dt : %s" %slc_start_dt)

        slc_end_dt = datetime(int(match.group('end_year')),
                              int(match.group('end_month')),
                              int(match.group('end_day')),
                              int(match.group('end_hour')),
                              int(match.group('end_min')),
                              int(match.group('end_sec')))

        #print("slc_end_dt : %s" %slc_end_dt)
	dt_orb = "%s_%s" % (slc_start_dt.isoformat(), slc_start_dt.isoformat())

        
 	if dt_orb not in orbit_dict:
            match = SLC_RE.search(slc_id)
            if not match:
                raise RuntimeError("Failed to recognize SLC ID %s." % slc_id)
            mission = match.group('mission')
     	    print(mission)
            orbit_url = fetch("%s.0" % slc_start_dt.isoformat(),
                                           "%s.0" % slc_end_dt.isoformat(),
                                           mission=mission, dry_run=True)

            orbit_dict[dt_orb] = orbit_url

            logger.info("REF_DT_ORB : %s VALUE : %s"%(dt_orb, orbit_dict[dt_orb]))
            if orbit_dict[dt_orb] is None:
                raise RuntimeError("Failed to query for an orbit URL for track {} {} {}.".format(track,
                                   slc_start_dt, slc_end_dt))

    except Exception as e:
	print(str(e))

    return orbit_url



def create_standard_product_job(project, auto_bbox, ifg_id, master_zip_url, master_orbit_url, 
                   slave_zip_url, slave_orbit_url, bbox, wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")

    job_type = "sentinel_ifg-singlescene"
    disk_usage = "300GB"

    # set job queue based on project
    job_queue = "%s-job_worker-large" % project

    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})

    return {
        "job_name": "%s-%s" % (job_type, ifg_id),
        "job_type": "job:%s" % job_type,
        "job_queue": job_queue,
        "container_mappings": {
            "/home/ops/.netrc": "/home/ops/.netrc",
            "/home/ops/.aws": "/home/ops/.aws",
            "/home/ops/ariamh/conf/settings.conf": "/home/ops/ariamh/conf/settings.conf"
        },    
        "soft_time_limit": 86400,
        "time_limit": 86700,
        "payload": {
            # sciflo tracking info
            "_sciflo_wuid": wuid,
            "_sciflo_job_num": job_num,

            # job params
            "project": project,
            "id": ifg_id,
            "master_zip_url": master_zip_url,
            "master_zip_file": [os.path.basename(i) for i in master_zip_url],
            "master_orbit_url": master_orbit_url,
            "master_orbit_file": os.path.basename(master_orbit_url),
            "slave_zip_url": slave_zip_url,
            "slave_zip_file": [os.path.basename(i) for i in slave_zip_url],
            "slave_orbit_url": slave_orbit_url,
            "slave_orbit_file": os.path.basename(slave_orbit_url),
            "swathnum": [1,2,3],
	    "azimuth_looks": 19,
  	    "range_looks" : 7,
	    "singlesceneOnly": true,
 	    "covth": 0.99,
	    "dem_type": dem_type,
	    "filter_strength": 0.5,
	    "job_priority": job_priority,
            "bbox": bbox,
            "auto_bbox": auto_bbox,

            # v2 cmd
            "_command": "/home/ops/ariamh/interferogram/sentinel/create_ifg_standard_product.sh",

            # disk usage
            "_disk_usage": disk_usage,

            # localize urls
            "localize_urls": localize_urls,
        }
    } 

if __name__ == "__main__":
    initiate_standard_product_job("_context.json")
