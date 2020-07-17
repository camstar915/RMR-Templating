import pandas as pd
import numpy as np
import time
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def run() :
    df = pd.read_csv("2020.07.16 T1's.csv", encoding='cp1252')
    print('CSV retrieved.')
    df1 = df.sort_values(['siteid'])
    print('Values sorted by siteid')
    df1.reset_index(inplace = True, drop = True)
    df1['sendnotices.1'] = df1['sendnotices.1'].fillna(0).astype(bool)
    df1['sendnotices'] = df1['sendnotices'].fillna(0).astype(bool)
    print('Filled null sendnotices row values with zeros')
    print('Changed sendnotice rows to booleans')
    targetColumns = ["utility", "billing", "notice", "envelope", "sId", "sCompany", "sAddress", "sCity", "sState", "sZip", "mCompany", "mContact", "mAddress", "mCity", "mState", "mZip", "hId1", "due1", "size1", "model1", "serial1", "cat1", "loc1", "testCompany1", "testCompanyPhone1", "hId2", "due2", "size2", "model2", "serial2", "cat2", "loc2", "testCompany2", "testCompanyPhone2", "hId3", "due3", "size3", "model3", "serial3", "cat3", "loc3", "testCompany3", "testCompanyPhone3", "hId4", "due4", "size4", "model4", "serial4", "cat4", "loc4", "testCompany4", "testCompanyPhone4", "hId5", "due5", "size5", "model5", "serial5", "cat5", "loc5", "testCompany5", "testCompanyPhone5", "hId6", "due6", "size6", "model6", "serial6", "cat6", "loc6", "testCompany6", "testCompanyPhone6", "hId7", "due7", "size7", "model7", "serial7", "cat7", "loc7", "testCompany7", "testCompanyPhone7", "hId8", "due8", "size8", "model8", "serial8", "cat8", "loc8", "testCompany8", "testCompanyPhone8", "hId9", "due9", "size9", "model9", "serial9", "cat9", "loc9", "testCompany9", "testCompanyPhone9", "hId10", "due10", "size10", "model10", "serial10", "cat10", "loc10", "testCompany10", "testCompanyPhone10", "hId11", "due11", "size11", "model11", "serial11", "cat11", "loc11", "testCompany11", "testCompanyPhone11", "hId12", "due12", "size12", "model12", "serial12", "cat12", "loc12", "testCompany12", "testCompanyPhone12", "hId13", "due13", "size13", "model13", "serial13", "cat13", "loc13", "testCompany13", "testCompanyPhone13", "hId14", "due14", "size14", "model14", "serial14", "cat14", "loc14", "testCompany14", "testCompanyPhone14", "hId15", "due15", "size15", "model15", "serial15", "cat15", "loc15", "testCompany15", "testCompanyPhone15", "hId16", "due16", "size16", "model16", "serial16", "cat16", "loc16", "testCompany16", "testCompanyPhone16", "hId17", "due17", "size17", "model17", "serial17", "cat17", "loc17", "testCompany17", "testCompanyPhone17", "hId18", "due18", "size18", "model18", "serial18", "cat18", "loc18", "testCompany18", "testCompanyPhone18", "hId19", "due19", "size19", "model19", "serial19", "cat19", "loc19", "testCompany19", "testCompanyPhone19", "hId20", "due20", "size20", "model20", "serial20", "cat20", "loc20", "testCompany20", "testCompanyPhone20", "hId21", "due21", "size21", "model21", "serial21", "cat21", "loc21", "testCompany21", "testCompanyPhone21", "hId22", "due22", "size22", "model22", "serial22", "cat22", "loc22", "testCompany22", "testCompanyPhone22", "hId23", "due23", "size23", "model23", "serial23", "cat23", "loc23", "testCompany23", "testCompanyPhone23", "hId24", "due24", "size24", "model24", "serial24", "cat24", "loc24", "testCompany24", "testCompanyPhone24", "hId25", "due25", "size25", "model25", "serial25", "cat25", "loc25", "testCompany25", "testCompanyPhone25", "hId26", "due26", "size26", "model26", "serial26", "cat26", "loc26", "testCompany26", "testCompanyPhone26", "hId27", "due27", "size27", "model27", "serial27", "cat27", "loc27", "testCompany27", "testCompanyPhone27", "hId28", "due28", "size28", "model28", "serial28", "cat28", "loc28", "testCompany28", "testCompanyPhone28", "hId29", "due29", "size29", "model29", "serial29", "cat29", "loc29", "testCompany29", "testCompanyPhone29", "hId30", "due30", "size30", "model30", "serial30", "cat30", "loc30", "testCompany30", "testCompanyPhone30", "hId31", "due31", "size31", "model31", "serial31", "cat31", "loc31", "testCompany31", "testCompanyPhone31", "hId32", "due32", "size32", "model32", "serial32", "cat32", "loc32", "testCompany32", "testCompanyPhone32", "hId33", "due33", "size33", "model33", "serial33", "cat33", "loc33", "testCompany33", "testCompanyPhone33", "hId34", "due34", "size34", "model34", "serial34", "cat34", "loc34", "testCompany34", "testCompanyPhone34", "hId35", "due35", "size35", "model35", "serial35", "cat35", "loc35", "testCompany35", "testCompanyPhone35"]
    dfTarget = pd.DataFrame(columns = targetColumns)
    dfFinal = pd.DataFrame(columns = targetColumns)
    print('Structured intermediary target dataframe')
    print('Structured final dataframe')
    utilityEnv = ['Archon', 'Barrington', 'Carson City', 'Cedar Falls', 'Elk Grove', 'Johnstown', 'Lawrence', 'Marion Utilities', 'Mill Creek', 'New Braunfels', 'Owensboro', 'Prosser', 'Richton Park', 'Signal Hill']
    print('Got list of cities that supply their own envelopes')
    userCheck = []
    userCheckSIDs = []
    tooManyHazards = []
    misplacedEmails = []
    misplacedEmailsIndex = []

    print('Deleting non-real utilities')
    df1 = df1[~df1.municipality.str.contains("Sample", na=False)]
    df1 = df1[~df1.municipality.str.contains("Demo", na=False)]
    df1 = df1[~df1.municipality.str.contains("NOT UTILITY", na=False)]
    df1 = df1[~df1.municipality.str.contains("Available", na=False)]
    # df1 = df1[~df1['address.1'].str.contains("@", na=False)]
    df1.reset_index(drop=True, inplace=True)

    dfSendSite = df1[df1['sendnotices']]
    for i in dfSendSite.index :
        if (dfSendSite.at[i, 'siteid'] in dfFinal['sId'].values) :
            continue
        else :
            dfTarget.loc[0, 'utility'] = dfSendSite.loc[i, 'municipality']
            dfTarget.loc[0, 'sId'] = dfSendSite.loc[i, 'siteid']
            dfTarget.loc[0, ['sCompany', 'mCompany']] = dfSendSite.loc[i, 'company']
            dfTarget.loc[0, 'mContact'] = dfSendSite.loc[i, 'contact']
            dfTarget.loc[0, ['sAddress', 'mAddress']] = dfSendSite.loc[i, 'address']
            dfTarget.loc[0, ['sCity', 'mCity']] = dfSendSite.loc[i, 'city']
            dfTarget.loc[0, ['sState', 'mState']] = dfSendSite.loc[i, 'state']
            dfTarget.loc[0, ['sZip', 'mZip']] = dfSendSite.loc[i, 'zip']
            dfTemp = dfSendSite[dfSendSite['siteid'] == dfSendSite.at[i, 'siteid']]
            dfTempTrimmed = dfTemp.drop_duplicates('hazid')
            if (len(dfTempTrimmed) > 20) :
                pd.set_option('display.max_rows', None)
                # print(dfTemp)
                # if (dfTempTrimmed.at[i, 'siteid'] not in tooManyHazards) :
                #     print('Site ' + str(dfTempTrimmed.at[i, 'siteid']) + ' had too many hazards to fit on one page')
                #     tooManyHazards.append(dfTempTrimmed.at[i, 'siteid'])
                if (dfTempTrimmed['siteid'].iloc[0] not in tooManyHazards) :
                    print('Site ' + str(dfTempTrimmed['siteid'].iloc[0]) + ' had too many hazards to fit on one page')
                    tooManyHazards.append(dfTempTrimmed['siteid'].iloc[0])
            else :
                for i in dfTempTrimmed.index :
                    ci = 16
                    while ci < 100000 :
                        if (np.isnan(dfTarget.at[0, dfTarget.columns[ci]])) :
                            dfTarget.at[0, dfTarget.columns[ci]] = dfTempTrimmed.at[i, 'hazid']
                            dfTarget.at[0, dfTarget.columns[ci + 1]] = dfTempTrimmed.at[i, 'testdue']
                            dfTarget.at[0, dfTarget.columns[ci + 2]] = dfTempTrimmed.at[i, 'devsize']
                            dfTarget.at[0, dfTarget.columns[ci + 3]] = dfTempTrimmed.at[i, 'model']
                            dfTarget.at[0, dfTarget.columns[ci + 4]] = dfTempTrimmed.at[i, 'serialnum']
                            dfTarget.at[0, dfTarget.columns[ci + 5]] = dfTempTrimmed.at[i, 'hazardcat']
                            dfTarget.at[0, dfTarget.columns[ci + 6]] = dfTempTrimmed.at[i, 'location']
                            dfTarget.at[0, dfTarget.columns[ci + 7]] = dfTempTrimmed.at[i, 'lasttestcompany']
                            dfTarget.at[0, dfTarget.columns[ci + 8]] = dfTempTrimmed.at[i, 'lasttestcompanyphone']
                            ci += 1000000
                        else :
                            ci += 9
                dfTarget.at[0, 'utility'] = dfSendSite.at[i, 'municipality']
                dfFinal = dfFinal.append(dfTarget, ignore_index = True)
                print('Added letter going to site: ' + str(dfTarget.at[0, 'sId']))
                dfTarget.drop(dfTarget.index[0], inplace = True)
    for i in df1.index :
        if df1.at[i, 'sendnotices.1'] :
            mask1 = df1['siteid'] == df1.at[i, 'siteid']
            mask2 = df1['sendnotices.1']
            dfTemp = df1[mask1 & mask2]
            for j in dfTemp.index :
                if (dfTemp.at[j, 'address.1'] in dfFinal[dfFinal['sId'] == dfTemp.at[j, 'siteid']]['mAddress'].values) :
                    continue
                else :
                    dfTarget.at[0, 'utility'] = dfTemp.at[j, 'municipality']
                    dfTarget.at[0, 'sId'] = dfTemp.at[j, 'siteid']
                    dfTarget.at[0, 'sCompany'] = dfTemp.at[j, 'company']
                    dfTarget.at[0, 'sAddress'] = dfTemp.at[j, 'address']
                    dfTarget.at[0, 'sCity'] = dfTemp.at[j, 'city']
                    dfTarget.at[0, 'sState'] = dfTemp.at[j, 'state']
                    dfTarget.at[0, 'sZip'] = dfTemp.at[j, 'zip']
                    dfTarget.at[0, 'mCompany'] = dfTemp.at[j, 'company.1']
                    dfTarget.at[0, 'mContact'] = dfTemp.at[j, 'contact.1']
                    dfTarget.at[0, 'mAddress'] = dfTemp.at[j, 'address.1']
                    dfTarget.at[0, 'mCity'] = dfTemp.at[j, 'city.1']
                    dfTarget.at[0, 'mState'] = dfTemp.at[j, 'state.1']
                    dfTarget.at[0, 'mZip'] = dfTemp.at[j, 'zip.1']
                    maskAddr = dfTemp['address.1'] == dfTemp.at[j, 'address.1']
                    dfTempTrimmed = dfTemp[maskAddr]
                    if (len(dfTempTrimmed) > 20) :
                        if (dfTempTrimmed.at[j, 'siteid'] not in tooManyHazards) :
                            print('Site ' + str(dfTempTrimmed.at[i, 'siteid']) + ' had too many hazards to fit on one page')
                            tooManyHazards.append(dfTempTrimmed.at[i, 'siteid'])
                    else :
                        for i in dfTempTrimmed.index :
                            ci = 16
                            while ci < 100000 :
                                if (np.isnan(dfTarget.at[0, dfTarget.columns[ci]])) :
                                    dfTarget.at[0, dfTarget.columns[ci]] = dfTempTrimmed.at[i, 'hazid']
                                    dfTarget.at[0, dfTarget.columns[ci + 1]] = dfTempTrimmed.at[i, 'testdue']
                                    dfTarget.at[0, dfTarget.columns[ci + 2]] = dfTempTrimmed.at[i, 'devsize']
                                    dfTarget.at[0, dfTarget.columns[ci + 3]] = dfTempTrimmed.at[i, 'model']
                                    dfTarget.at[0, dfTarget.columns[ci + 4]] = dfTempTrimmed.at[i, 'serialnum']
                                    dfTarget.at[0, dfTarget.columns[ci + 5]] = dfTempTrimmed.at[i, 'hazardcat']
                                    dfTarget.at[0, dfTarget.columns[ci + 6]] = dfTempTrimmed.at[i, 'location']
                                    dfTarget.at[0, dfTarget.columns[ci + 7]] = dfTempTrimmed.at[i, 'lasttestcompany']
                                    dfTarget.at[0, dfTarget.columns[ci + 8]] = dfTempTrimmed.at[i, 'lasttestcompanyphone']
                                    ci += 1000000
                                else :
                                    ci += 9
                        dfFinal = dfFinal.append(dfTarget, ignore_index = True)
                        print('Added letter going to mailing address: ' + str(dfTarget.at[0, 'mAddress']))
                        dfTarget.drop(dfTarget.index[0], inplace = True)
    mask1 = df1['sendnotices'] == False
    mask2 = df1['sendnotices.1'] == False
    dfBothZero = df1[mask1 & mask2]
    for i in dfBothZero.index :
        if (dfBothZero.at[i, 'siteid'] in dfFinal['sId'].values) :
            continue
        else :
            dfTarget.loc[0, 'utility'] = dfBothZero.loc[i, 'municipality']
            dfTarget.loc[0, 'sId'] = dfBothZero.loc[i, 'siteid']
            dfTarget.loc[0, ['sCompany', 'mCompany']] = dfBothZero.loc[i, 'company']
            dfTarget.loc[0, 'mContact'] = dfBothZero.loc[i, 'contact']
            dfTarget.loc[0, ['sAddress', 'mAddress']] = dfBothZero.loc[i, 'address']
            dfTarget.loc[0, ['sCity', 'mCity']] = dfBothZero.loc[i, 'city']
            dfTarget.loc[0, ['sState', 'mState']] = dfBothZero.loc[i, 'state']
            dfTarget.loc[0, ['sZip', 'mZip']] = dfBothZero.loc[i, 'zip']
            dfTemp = dfBothZero[dfBothZero['siteid'] == dfBothZero.at[i, 'siteid']]
            dfTempTrimmed = dfTemp.drop_duplicates('hazid')
            if (len(dfTempTrimmed) > 20) :
                if (dfTempTrimmed.at[i, 'siteid'] not in tooManyHazards) :
                    print('Site ' + str(dfTempTrimmed.at[i, 'siteid']) + ' had too many hazards to fit on one page')
                    tooManyHazards.append(dfTempTrimmed.at[i, 'siteid'])
            else :
                for i in dfTempTrimmed.index :
                    ci = 16
                    while ci < 100000 :
                        if (np.isnan(dfTarget.at[0, dfTarget.columns[ci]])) :
                            dfTarget.at[0, dfTarget.columns[ci]] = dfTempTrimmed.at[i, 'hazid']
                            dfTarget.at[0, dfTarget.columns[ci + 1]] = dfTempTrimmed.at[i, 'testdue']
                            dfTarget.at[0, dfTarget.columns[ci + 2]] = dfTempTrimmed.at[i, 'devsize']
                            dfTarget.at[0, dfTarget.columns[ci + 3]] = dfTempTrimmed.at[i, 'model']
                            dfTarget.at[0, dfTarget.columns[ci + 4]] = dfTempTrimmed.at[i, 'serialnum']
                            dfTarget.at[0, dfTarget.columns[ci + 5]] = dfTempTrimmed.at[i, 'hazardcat']
                            dfTarget.at[0, dfTarget.columns[ci + 6]] = dfTempTrimmed.at[i, 'location']
                            dfTarget.at[0, dfTarget.columns[ci + 7]] = dfTempTrimmed.at[i, 'lasttestcompany']
                            dfTarget.at[0, dfTarget.columns[ci + 8]] = dfTempTrimmed.at[i, 'lasttestcompanyphone']
                            ci += 1000000
                        else :
                            ci += 9
                dfTarget.at[0, 'utility'] = dfBothZero.at[i, 'municipality']
                dfFinal = dfFinal.append(dfTarget, ignore_index = True)
                print('\'sendnotice\' field was 0 for both site and hazard. Added letter going to site: ' + str(dfTarget.at[0, 'sId']))
                dfTarget.drop(dfTarget.index[0], inplace = True)
    for i in dfFinal.index :
        mAddress = dfFinal.at[i, 'mAddress']
        mContact = str(dfFinal.at[i, 'mContact'])
        siteid = dfFinal.at[i, 'sId']
        if '@' in mContact or '@' in mAddress:
            if not siteid in misplacedEmails :
                misplacedEmails.append(siteid)
                misplacedEmailsIndex.append(i)
    dfFinal.drop(index = misplacedEmailsIndex, inplace = True)
    dfFinal.reset_index(drop = True, inplace = True)
    for i in dfFinal.index :
        if dfFinal.at[i, 'utility'].startswith('Archon') :
            dfFinal.at[i, 'billing'] = 'Archon'
        else :
            dfFinal.at[i, 'billing'] = 'ABF'
    dfFinal['envelope'] = 'ABF'
    for i in dfFinal[dfFinal['utility'].str.contains('|'.join(utilityEnv))].index :
        print('Setting envelope for ' + str(dfFinal.at[i, 'utility']))
        dfFinal.at[i, 'envelope'] = dfFinal.at[i, 'utility']
    print('Set billing to ABF or Archon')
    dfFinal.sort_values(['sId'], inplace=True)
    dfFinal.reset_index(drop=True, inplace=True)
    print('Saving file...')
    dfFinal.to_excel('output.xlsx')
    print(' ')
    print(' ')
    print(' ')
    print(' ')
    print(' ')
    print(' ')
    dfOnlyDups = dfFinal[dfFinal.duplicated(subset = 'sId', keep = False)]
    for i in dfOnlyDups.index :
        maskSID = dfOnlyDups['sId'] == dfOnlyDups.at[i, 'sId']
        dfTrimmed = dfOnlyDups[maskSID]
        for j in dfTrimmed.index :
            for x in dfTrimmed.index :
                if (j == x) :
                    continue
                if (fuzz.ratio(dfTrimmed.at[j, 'mAddress'].lower(), dfTrimmed.at[x, 'mAddress'].lower()) > 60) :
                    if (fuzz.ratio(dfTrimmed.at[j, 'mCity'].lower(), dfTrimmed.at[x, 'mCity'].lower()) < 90) :
                        continue
                    elif not (dfTrimmed.at[j, 'sId'] in userCheckSIDs) :
                        userCheckSIDs.append(dfTrimmed.at[j, 'sId'])
                        userCheck.append({'sId':dfTrimmed.at[j, 'sId'], 'm1':dfTrimmed.at[j, 'mAddress'], 'm2':dfTrimmed.at[x, 'mAddress'], 'mC1':dfTrimmed.at[j, 'mCompany'], 'mC2':dfTrimmed.at[x, 'mCompany']})
                    x += 1
                x += 1
    dash = '-' * 100
    print('\n\n\n' + dash)
    print('Done!\nYour new file is named \'output.xlsx\' \nThese addresses looked similar')
    print(dash)
    print('\n\n')
    print('{:<10}{:<50}{:>40}'.format('Site ID', 'Mailing Addresses', 'Mailing Company')+'\n')
    for item in userCheck :
        print('{:<10}{:<50}{:>40}'.format(item['sId'], item['m1'], item['mC1']))
        print('{:<10}{:<50}{:>40}'.format('', item['m2'], item['mC2'])+'\n')
    print(' ')
    print(' ')
    print(' ')
    print('Check these ^ sites for potential duplicate mailing addresses')
    print('\nThe following sites had too many hazards to send to RMR:')
    for site in tooManyHazards :
        print(site)
    print('\nThe following sites had misplaced email addresses, and had to be removed from the mailing:')
    for i in misplacedEmails :
        print(str(i) + ' ', end = '')
    print('\nHave a wonderful day!')

print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print('Before continuing:')
print(' ')
print('1. Save the file you\'d like to transform in the same folder as this program.')
print('2. Name the file \"tmbExport\" and make sure it is a CSV')
print('3. Make sure the file has the following column names')
print(' ')
print('municipality - siteid - company - contact - address - city - state - zip - sendnotices - company - contact - address - city - state - zip - sendnotices - hazid - testdue - devsize - model - serialnum - hazardcat - location - lasttestcompany - lasttestcompanyphone')
print(' ')
print(' ')
x = input('Are you ready to continue? (y/n)')
if x == 'n' or x == 'N' :
    print('ok....')
    time.sleep(3)
    print(' ')
    x1 = input('Now are you ready?... (y/n)')
    if x1 == 'y' or x1 == 'Y' :
        print('Finally...')
        run()
    else :
        print('Well you can start this program again when you\'re ready. I\'m going back to sleep')
        exit()
elif x == 'y' or x == 'Y' :
    print('Starting...')
    run()
else :
    print(' ')
    x1 = input('I SAID TO TYPE \'Y\' OR \'N\'! Try again...')
    if x1 == 'y' or x1 == 'Y' :
        print('That\'s better... Starting...')
        run()
    elif x1 == 'n' or x1 == 'N' :
        print('Why\'d you even start the program then... I\'m leaving.')
        exit()
    else :
        print('Well you let me know when you\'re ready to cooperate.')
        exit()
