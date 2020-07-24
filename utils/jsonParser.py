'''
json parser
'''
import sys
def jsonLoad(path):
    with open(path,'r+',encoding='utf-8') as f:  #打开json文件
        datas = f.readlines()   #将json文件所有行读出，存储为list，每一个元素为字符串后的dict
    datas = [eval(data) for data in datas] #将字符串（字典）转为dict
    print('data length:',len(datas))    #打印评论长度
    return datas

def jsonSaver(data_list,path,flg):
    if(flg==0): # 第一页评论使用覆盖方式保存，避免同首歌曲评论重复写入文件
        f = open(path,'w+',encoding='utf-8')
    else:
        f = open(path, 'a+', encoding='utf-8')
    for d in data_list:
        f.write(str(d)+'\n')
    f.close()

def saveSongInfo(song_id,song_name,song_author):
    Info = {'songID':song_id,'songName':song_name,'songAuthor':song_author}
    with open('./data/SongInfo.json','a+',encoding='utf-8') as f:
        f.write(str(Info))
        f.write('\n')

if __name__ == '__main__':
    pass
