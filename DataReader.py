import pandas as pd
import os
import re


class DataReader(object):

    def __init__(self):
        pass
    


    @staticmethod
    def _filename_pattern(pattern = None, work_dir = os.getcwd()):
        """
        Select all files that follow the same pattern, called by read_batch_data
        """
        f_ = os.listdir(work_dir)
        regex_pattern = re.compile(pattern, re.I)
        regex_match = lambda x: regex_pattern.fullmatch(x)
        files_out = []
        for i in range(len(f_)):
            if pattern == None:
                files_out.append(f_[i])
            else:
                regex_str = regex_match(f_[i])
                if regex_str != None:
                    files_out.append(regex_str.string)
        print('{} files read'.format(len(files_out)))
    
        if len(files_out) == 0:
            raise BaseException('No file read')
    
        return files_out
        
    @staticmethod
    def read_regex(read_str, pattern = None, work_dir = os.getcwd(), desc = False, **options):
        """
        Read batch data following some pattern and stack them.
        The order of reading is sorted by string (default smallest first)
        options stores read options
        read_str are pandas reading functions
        """
        regex_filename = _filename_pattern(pattern = pattern, work_dir = work_dir)
        regex_filename.sort(reverse = desc)
    
        dfs = []
        for i in regex_filename:
            exec_str_ = 'df_ = ' + read_str + '("{}", **options)'.format(os.path.join(work_dir,i))
            local_ = {'options':options}
            exec(exec_str_, globals(), local_)
            dfs.append(local_['df_'])
        
        df = pd.concat(dfs)
        return df

        
        

if __name__ == '__main__':


    df1 = pd.DataFrame({'a':[1,2,3],'b':[4,5,6]})
    df2 = pd.DataFrame({'a':[1,2,4],'b':[7,8,9]})
    
    df1.to_csv('2017.csv')
    df1.to_csv('2018.csv')
    df2.to_csv('2019.csv')
    
    
    #test _filename_pattern
    pattern = '^[0-9]{4}.csv$' 
    df = DataReader.read_regex('pd.read_csv', pattern = pattern)
