from antares_client import StreamingClient
from statsmodels.stats.weightstats import DescrStatsW
import os
import numpy as np
from google.cloud import logging

# Instantiates the logging client
logging_client = logging.Client()
log_name = "alert-streaming-client"
logger = logging_client.logger(log_name)

TOPICS = ["high_flux_ratio_wrt_nn"]
CONFIG = {
    "api_key": os.environ["API_KEY"],
    "api_secret": os.environ["API_SECRET"],
}
# class def here

def process_alert(topic, locus):
    """Put your code here!"""
    if locus.alert.properties['ant_survey'] != 1: #be sure we aren't triggered with upper limits
        print ("up lim")
        return 
        
    threshold = 0.5  # in magnitude unit, and same for all filters
    fid = locus.alert.properties['ztf_fid']

    df = locus.timeseries
    
    mask = (df['ztf_fid'] == fid) & (df['ant_survey'] == 1) #expect both columns to be unmasked
    df = df[mask]  
    
    
    if len(df) < 2: # Locus has < 2 measurements with fid matching the current alert fid.
        print ('too few points')
        return

    corrected = False
    mag = df['ztf_magpsf'].data #expect unmasked
    magerr = df['ztf_sigmapsf'].data #expect unmasked
    
    if isinstance(df['ant_mag_corrected'], MaskedColumn) == False:
        corrected = True
        mag = df['ant_mag_corrected'].data 
        magerr = df['ant_magerr_corrected'].data 
    else:
        if np.sum(df['ant_mag_corrected'].mask) < len(df):
            corrected = True
            
            mag = df['ant_mag_corrected'].filled(fill_value=np.nan).data 
            mm = np.isfinite(mag)
            mag = mag[mm]
            
            magerr = df['ant_magerr_corrected'].filled(fill_value=np.nan).data
            mm = np.isfinite(magerr)
            magerr = magerr[mm]
                    
    
    is_var_star = self._is_var_star(df) ## only one filter type passed

    if is_var_star == True and corrected == True:
        tag = 'high_amplitude_variable_star_candidate'
    if is_var_star == True and corrected == False:  #if uncorrected but var_star, we skip
        return 
    if is_var_star == False:
        tag = 'high_amplitude_transient_candidate'

    alert_id = locus.alert.alert_id  # current alert_id
    ztf_object_id = locus.properties['ztf_object_id']  # ZTF Object ID
    
    W = 1.0 / magerr ** 2.0
    des = DescrStatsW(mag, weights=W)
    #print (is_var_star, corrected, des.std, des.mean, tag)
    
    if des.std > threshold:
        #print (f'hit!!! {tag} {alert_id} {ztf_object_id}')
        locus.tag(tag)
        logger.log_text(locus.tag(tag)) # log alert
    pass


def main():
   with StreamingClient(TOPICS, **CONFIG) as client:
       for topic, locus in client.iter():
           process_alert(topic, locus)


if __name__ == "__main__":
    main()
