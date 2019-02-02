# Case Reserve Simulation 

# Import Libraries
import itertools
import random
import simpy
import pandas as pd
from pandasql import sqldf
from functools import partial, wraps
import math
import sys
import os
print ('Imported Libraries')

#For Deep Diving by plotting
import matplotlib.pyplot as plt
print ('Imported plotting Libraries')

# Truncate Output files
writeheader = True
odpath = sys.argv[2] + '/ric_' + sys.argv[3] + '_roc_' + sys.argv[4] + '_' + sys.argv[5]+ '/' #'C:/Users/ankitjai/Desktop/simulator_v2/testing/test1asin/outv1/' 

if not os.path.exists(odpath): # create output directory if does not exist
    os.makedirs(odpath)
	
open(odpath + 't1p_replen.csv', 'w').close()
open(odpath + 't1p_prime.csv', 'w').close()
open(odpath + 't1p_reserve.csv', 'w').close()
open(odpath + 't1p_success_asin.csv','w').close()
open(odpath + 't1p_fail_asin.csv','w').close()

#Simulation Parameters
rin = sys.argv[3] #'4wk'  #Reserve in threshold in weeks
rout = sys.argv[4] #'1wk'   #reserve out threshold in weeks
review_time = 24 # Assumption on how often inventory will be reviewed for replenishment
sim_run_time = 2857
print ('Read Sim Parameters')

# Read Data
chunksize = 3336000 # Read data more than size of memory by chunking
filename = sys.argv[1] #'testasin.csv'  
for df1 in pd.read_table(filename,sep = '\t',chunksize=chunksize,dtype={'FNSKU': object}):
#     pd.read_table('top30kasin.txt',sep = '\t',chunksize=chunksize,dtype={'FNSKU': object})
    df1.columns = map(str.lower, df1.columns) # convert to lower case
    #df1.loc[df1.t==sim_run_time +10,'outbound'] =10 # To avoid indexing error  X000DV5RJL 0986034762
    #df1.loc[df1.t==sim_run_time+10,'inbound'] =10  # To avoid indexing error when no data in inbound or outbound
    fin = 'fcst_' + rin
    fout = 'fcst_' + rout
    df1['ric'] = df1[fin]*df1['fc_prop'] # Time Series of Reserve in Criteria
    df1['roc'] = df1[fout]*df1['fc_prop'] # Time Series of Reserve Out Criteria 3 days
    fnsku_list = df1.fnsku.unique()
    fnsku_count = df1.fnsku.nunique()
    print ('Read dataframe of size %d with %d ASIN'%(len(df1),fnsku_count))
    
    # Get Data for a ASIN
    for item in fnsku_list:
        try:
            df = df1[df1['fnsku']==item].sort_values(['t'],ascending =[True]) # Sort by time
            df_inbound = df[df['inbound']>0].sort_values('t') # Inbound Time Series
            df_outbound = df[df['outbound']>0].sort_values('t') #Outbound Time Series
            print ('Created data frame for fnsku %s' %(item))

            # Data Transformation for simulation
            df_outbound['tprev'] = df_outbound.t.shift(1) # lag 1 row 
            df_outbound['gap'] = df_outbound['t'] - df_outbound['tprev'] # Time between two outbound events
            df_outbound['gap'] = df_outbound['gap'].fillna(df_outbound['t']) # First row fill up with actual time

            df_inbound['tprev'] = df_inbound.t.shift(1)
            df_inbound['gap'] = df_inbound['t'] - df_inbound['tprev'] 
            df_inbound['gap'] = df_inbound['gap'].fillna(df_inbound['t']) 
            print ('Created additional columns in data frame')


            # Parameters
            cq = math.ceil(1/df.iloc[0]['vol']) #carton quantity
            mrq = cq #Min replenish quantity
            
            if pd.isnull(df.iloc[0]['ric']) == 1:
                if sys.argv[6] == 'nullprime':
                    iric =  10000000  # if forecast null and user specified to go in prime assign high threshold
                else:
                    iric = 0 # default behavior

            else:
                iric = max(df.iloc[0]['ric'],cq) # initial reserve in criteria. At least more than cq. 
                
            if pd.isnull(df.iloc[0]['roc']) == 1:
                iroc = iric
            else:
                iroc = max(df.iloc[0]['roc'],cq) #initial reserve out criteria
            
            if sys.argv[5] =='redist':
                prime_initial = 0 
            else:
                prime_initial = df.iloc[0]['onhand_init'] # Initial prime inventory,for redistribute scenario modify this to 0
            
            reserve_initial = 0 # Initial Reserve Inventory
            
            print ('Carton Quantity %f , mrq %f , iric %f, iroc %f' %(cq,mrq,iric,iroc))

            # Capture Sim Data
            ob_data = [] #List for holding outbound values
            ib_data =[] # List for holding inbound data
            replen_amt =[] # List for holding replen amount 
            data_level_prime = [] # Prime Container Level
            data_level_reserve = [] # Reserve Container Level
            event_data_level_prime = [] # Event based Prime Container Level
            event_data_level_reserve = [] # Event based Reserve Container Level
            # Define Processes inside FC

            class fc1: #http://simpy.readthedocs.io/en/latest/topical_guides/resources.html#res-type-container
                def __init__(self, env):
                    self.prime_tank = simpy.Container(env, init=prime_initial, capacity=100000000)
                    self.reserve_tank = simpy.Container(env, init=reserve_initial, capacity=100000000)
                    self.mon_proc = env.process(self.monitor_tank(env))
                    print('Defined container types')

                def monitor_tank(self, env): # Monitoring prime tank
                     while True:
                        if len(df_outbound.index)==0:
                            yield env.timeout(sim_run_time)
                        else:
                            df_outbound['td'] = abs(df_outbound['t'] - env.now)
                            repdf = df_outbound.sort_values(by = ['td'], ascending=[1])
                            rtime = repdf.iloc[0]['gap'] 
                        
                        if env.now ==0:
                            roc = iroc
                        else:    
                            troc = repdf.iloc[0]['roc'] 
                            if pd.isnull(troc) == 1:
                                roc = iric
                            else:
                                roc = max(troc,cq) # min 1 cq in prime                               
    #                     print roc
                        if self.prime_tank.level < roc:
                            print('Calling replen at %s with prime inventory at %d and roc at %d' %(env.now,self.prime_tank.level,roc))
                            env.process(replen(env,self,roc))
                    
                        interval = max(review_time,rtime) #speed up monitoring for ASIN with sparse data
                        yield env.timeout(interval) # Assumption on inventory review time

            def replen(env,fc,roc): # Replen Process
                print ('Reached Replen at %s' %env.now)
                desired = roc - fc.prime_tank.level
                if desired >0:
                    ccq = max((math.ceil(desired / cq))*cq,mrq) #case constrained quantity
                else:
                    ccq = 0 
                print ('Desired is %f and ccq is %f'%(desired, ccq))
                amount = max(0,min(fc.reserve_tank.level,ccq)) 
                print ('Replen amount is %d'%(amount))
                if amount == 0:
                    print ('No quantity in reserve')
                else:
                    yield fc.reserve_tank.get(amount)
                    rep_time = random.randint(12,24)
                    yield env.timeout(rep_time) # time taken to replenish
                    yield fc.prime_tank.put(amount)
                    print('Replen at time %s of %d units to prime in %d hours' % (env.now,amount,rep_time))
                    ir = (env.now,amount,item)
                    replen_amt.append(ir)
                print ('Done Replen at %s' %(env.now))


            def redistinv(env,fc,ric): # Redistribute Inventory
                print ('Reached Intitialize')
                min_prime_qty = cq
                prime_redistribute = min(max(ric,min_prime_qty),df.iloc[0]['onhand_init'])
                print prime_redistribute
                reserve_redistribute = max(0,df.iloc[0]['onhand_init'] - prime_redistribute)
                if prime_redistribute ==0:
                    g1 =1
                    print ('No inventory goes to prime while redistribution')
                else:
                    yield fc.prime_tank.put(prime_redistribute)

                if reserve_redistribute ==0:
                    g1 =1
                    print ('No inventory goes to reserve while redistribution')
                else:
                    yield fc.reserve_tank.put(reserve_redistribute)
                print ('Redistributed Inv in prime %d and in reserve %d' % (prime_redistribute,reserve_redistribute))   


            def outbound(fnsku,env,fc,ob_qty): # Outbound Process
                print ('Reached outbound at %s'%(env.now))
                ob1_prime = max(0,min(fc.prime_tank.level,ob_qty))
                ob1_reserve = max(0,min(fc.reserve_tank.level,ob_qty - ob1_prime))
                print ('Outbound %d, prime %d, reserve%d'%(ob_qty,ob1_prime, ob1_reserve))
                if ob1_prime ==0:
                    g1 =1
                    print ('No inventory in Prime to outbound')
                else:    
                    yield fc.prime_tank.get(ob1_prime)

                if ob1_reserve==0:
                    g1 =1
                else:
                    yield fc.reserve_tank.get(ob1_reserve)
                    print ('Outbound of %d units from reserve'%(ob1_reserve))

                ship_out = ob1_prime + ob1_reserve
                backlog = ob_qty - ship_out
                m1 = (env.now,ob_qty,ob1_prime,ob1_reserve,ship_out,backlog,item)
                ob_data.append(m1)
                print ('Done Outbound')


            def inbound(fnsku,env,fc,inb_qty,ric): # Inbound Process
                print ('Reached Inbound at %s to inbound %d with ric of %d' %(env.now,inb_qty,ric))
                total = fc.prime_tank.level + inb_qty
                min_prime_qty = cq
                des_res_qty = total - max(ric,min_prime_qty) # double check at least 1 cq in prime
                cc_des_res_qty = (math.floor(des_res_qty/cq))*cq # carton constrained desired case reserve inbound quantity
                res_qty = max(0,min(inb_qty,cc_des_res_qty))
                prime_qty = max(0,inb_qty - res_qty)
                print ('Onhand Prime %d Inbound %d  Prime %d Reserve %d'%(fc.prime_tank.level,inb_qty,prime_qty,res_qty))
                if prime_qty ==0:
                    g1=1
                    print ('No inventory goes in Prime')
                else:    
                    yield fc.prime_tank.put(prime_qty)
                    print ('Done putting inventory in Prime')
                if res_qty ==0:
                    print ('No inventory goes in Reserve')
                else:        
                    yield fc.reserve_tank.put(res_qty)
                    print ('Done putting inventory in Reserve')
                m2 = (env.now,inb_qty,prime_qty,res_qty,item)
                ib_data.append(m2)
                print ('Done inbound')


            def outbound_generator(env,fc):
                print ('Reached Outbound Generator at %s'%(env.now))
                b = len(df_outbound[df_outbound['t']<=sim_run_time].index)
                for i in range(b):
                    otime = df_outbound.iloc[i]['gap']
                    print ('time gap of %d and current time is %s'%(otime, env.now))
                    yield env.timeout(otime)
                    obgen = df_outbound.iloc[i]['outbound']
                    print ('Outbound generated at %s for %d units'%(env.now,obgen))
                    fnsku =item
                    env.process(outbound(fnsku,env,fc,obgen))

            def inbound_generator(env,fc):
                print ('Reached Inbound Generator at %s'%(env.now))
                a = len(df_inbound[df_inbound['t']<=sim_run_time].index) # identify number of inbound events
                for i in range(a):
                    gap = df_inbound.iloc[i]['gap']
                    print ('Inbound interval of %d'%(gap))
                    yield env.timeout(gap)
                    ingen = df_inbound.iloc[i]['inbound']
                    tinric = df_inbound.iloc[i]['ric']
                    if pd.isnull(tinric) == 1:
                        if sys.argv[6] == 'nullprime':
                            inric =  10000000  # if forecast null and user specified to go in prime assign high threshold
                        else:
                            inric = 0 # default behavior  
                    else:
                        inric = max(cq,tinric)
                    print ('Inbound Generated at %s for %s of %d units with ric of %f'%(env.now,item,ingen,inric))
                    env.process(inbound(item,env,fc,ingen,inric)) 

            #Monitoring Code
            def patch_resource(resource, pre=None, post=None):
                """Patch *resource* so that it calls the callable *pre* before each
                put/get/request/release operation and the callable *post* after each
                operation.  The only argument to these functions is the resource
                instance.
                """
                def get_wrapper(func):
                    # Generate a wrapper for put/get/request/release
                    @wraps(func)
                    def wrapper(*args, **kwargs):
                    # This is the actual wrapper
                    # Call "pre" callback
                        if pre:
                            pre(resource)

                    # Perform actual operation
                        ret = func(*args, **kwargs)

                    # Call "post" callback
                        if post:
                            post(resource)

                        return ret
                    return wrapper

            # Replace the original operations with our wrapper
                for name in ['put', 'get', 'request', 'release']:
                    if hasattr(resource, name):
                        setattr(resource, name, get_wrapper(getattr(resource, name)))

            def monitor(data, resource):
                """This is our monitoring callback."""
                m3 = (
                resource._env.now,  # The current simulation time
                resource.level,  # The level of container
                item     
                )
                data.append(m3)

            def periodic_sampler(env,fc,data_level_prime,data_level_reserve):
                print ('Reached Periodic Sampler')
                while True:
                    pitem = (env.now,fc.prime_tank.level,item)
                    ritem = (env.now,fc.reserve_tank.level,item)
                    data_level_prime.append(pitem)
                    data_level_reserve.append(ritem)
                    gap = 24*7
                    yield env.timeout(gap)

            # Start Processes
            env = simpy.Environment()
            fc = fc1(env)
            if sys.argv[5] =='redist':
                redist = env.process(redistinv(env,fc,iric)) # Redistribute inventory
            env.process(periodic_sampler(env,fc,data_level_prime,data_level_reserve))
            env.process(outbound_generator(env,fc))
            env.process(inbound_generator(env,fc))

            # Event based monitoring to capture detailed events that can be missed in periodic sampling
            # Bind *data* as first argument to monitor see https://docs.python.org/3/library/functools.html#functools.partial
            monitor_level_reserve = partial(monitor, event_data_level_reserve)
            monitor_level_prime = partial(monitor, event_data_level_prime)
            patch_resource(fc.reserve_tank, post=monitor_level_reserve)  # Patches (only) this resource instance
            patch_resource(fc.prime_tank, post=monitor_level_prime)  # Patches (only) this resource instance

            #Start Simulation
            env.run(sim_run_time)

            #Get Metrics
            # Transform data to Data Frames
            context = df.iloc[0]['fc'] + '_ric' + sys.argv[3] + '_roc' + sys.argv[4] + '_' + sys.argv[5]
            #Processes
            df_replen = pd.DataFrame(replen_amt, columns = ['t','quantity','fnsku'])
            df_ob = pd.DataFrame(ob_data, columns = ['t','ob_qty','ob1_prime','ob1_reserve','ship_out','backlog','fnsku'])
            df_ib = pd.DataFrame(ib_data, columns = ['t','in_quantity','prime','reserve','fnsku'])
            df_replen['context'] = context
            
            #Containers
            df_prime_level = pd.DataFrame(data_level_prime, columns = ['t','quantity','fnsku'])
            df_reserve_level = pd.DataFrame(data_level_reserve, columns = ['t','quantity','fnsku'])
            #df_prime_level_event = pd.DataFrame(event_data_level_prime, columns = ['t','quantity','fnsku'])
            #df_reserve_level_event = pd.DataFrame(event_data_level_reserve, columns = ['t','quantity','fnsku'])
            df_prime_level['context'] = context
            df_reserve_level['context'] = context
            
            
            #ASIN metrics
            rp = df_replen.quantity.sum()
            ob = df_ob.ob_qty.sum()
            ob_prime = df_ob.ob1_prime.sum()
            ob_reserve = df_ob.ob1_reserve.sum()
            ship_out = df_ob.ship_out.sum()
            backlog = df_ob.backlog.sum()
            ib = df_ib.in_quantity.sum()
            ib_reserve = df_ib.reserve.sum()
            ib_prime = df_ib.prime.sum()
            prime_start_inv = df_prime_level.loc[0,'quantity']
            prime_end_inv   = df_prime_level.loc[len(df_prime_level)-1,'quantity']
            prime_average_inv = df_prime_level.quantity.mean()
            reserve_start_inv = df_reserve_level.loc[0,'quantity']
            reserve_end_inv   = df_reserve_level.loc[len(df_reserve_level)-1,'quantity']
            reserve_average_inv = df_reserve_level.quantity.mean()
            total_end_inv = prime_end_inv + reserve_end_inv
            vol = df.iloc[0]['vol']
            fnsku_type = df.iloc[0]['fnsku_type']
            onhand_init = df.iloc[0]['onhand_init']
            

            p1 = (item,rp,ob,ob_prime,ob_reserve,ship_out,backlog,ib,ib_reserve,ib_prime
                  ,prime_start_inv,prime_end_inv,prime_average_inv,reserve_start_inv,reserve_end_inv,reserve_average_inv,total_end_inv,vol
                  ,fnsku_type,onhand_init,context)
       
            success_asin=[]
            success_asin.append(p1)
            df_success_asin = pd.DataFrame(success_asin,columns=['item','rp','ob','ob_prime','ob_reserve','ship_out','backlog'
                                                                 ,'ib','ib_reserve','ib_prime','prime_start_inv','prime_end_inv'
                                                                 ,'prime_average_inv','reserve_start_inv','reserve_end_inv'
                                                                 ,'reserve_average_inv','total_end_inv','vol'
                                                                 ,'fnsku_type','onhand_init','context'])
        
         # Export Data
            if writeheader is True:
                df_replen.to_csv(odpath + 't1p_replen.csv', columns =['t','quantity','fnsku','context'], mode='a', header=True,index = False)
                df_prime_level.to_csv(odpath + 't1p_prime.csv', columns =['t','quantity','fnsku','context'], mode='a', header=True,index = False)
                df_reserve_level.to_csv(odpath + 't1p_reserve.csv', columns =['t','quantity','fnsku','context'], mode='a', header=True,index = False)
                df_success_asin.to_csv(odpath + 't1p_success_asin.csv',columns=['item','rp','ob','ob_prime','ob_reserve','ship_out','backlog'
                             ,'ib','ib_reserve','ib_prime','prime_start_inv','prime_end_inv'
                             ,'prime_average_inv','reserve_start_inv','reserve_end_inv'
                            ,'reserve_average_inv','total_end_inv','vol','fnsku_type','onhand_init','context']
                             ,mode='a', header=True,index = False )
                writeheader = False
            else:
                df_replen.to_csv(odpath + 't1p_replen.csv', columns =['t','quantity','fnsku','context'], mode='a', header=False,index = False)
                df_prime_level.to_csv(odpath + 't1p_prime.csv', columns =['t','quantity','fnsku','context'], mode='a', header=False,index = False)
                df_reserve_level.to_csv(odpath + 't1p_reserve.csv', columns =['t','quantity','fnsku','context'], mode='a', header=False,index = False)
                df_success_asin.to_csv(odpath + 't1p_success_asin.csv',columns=['item','rp','ob','ob_prime','ob_reserve','ship_out','backlog'
                                ,'ib','ib_reserve','ib_prime','prime_start_inv','prime_end_inv'
                                 ,'prime_average_inv','reserve_start_inv','reserve_end_inv'
                                ,'reserve_average_inv','total_end_inv','vol','fnsku_type','onhand_init','context']
                            ,mode='a', header=False,index = False )
 
        except:
            print ('Encountered Exception at %s' %(env.now))
            fail_asin=[]
            fail_asin.append(item)
            df_fail_asin = pd.DataFrame(fail_asin,columns=['fnsku'])
            df_fail_asin.to_csv(odpath + 't1p_fail_asin.csv', columns =['fnsku'], mode='a', header=False,index = False)
            
