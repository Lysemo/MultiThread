'''
func:通过多线程并行方式爬取网页云评论数据.
author:Lele Wu.
time:2020/07/24 22:38
-------------------------
硬件检测：
--4线程
--RAM占用 稳定在42%(16GB)
--CPU占用 跳动在[20,60](8300H)
--DISK占用 稳定1%(SSD 512)
'''
from selenium import webdriver
import copy
import time
import threading
from utils.jsonParser import saveSongInfo,jsonSaver
from utils.CrawImg import CrawImg

# 按钮点击
def scriptClick(bro,page):
    bro.execute_script('arguments[0].click();', page)
    time.sleep(2)

# 保存用户头像
def saveAvatar(url,user_id):
    crawImg = CrawImg()
    crawImg.getImg(url)
    imgName = 'data/avatar/' + user_id + crawImg.getArray()[0].getSuffixName()
    with open(imgName,'wb+') as f:
        f.write(crawImg.getArray()[0].getImg())

# 评论解析
def commentsParser(comments,song_id):
    comment_list = []
    template_dict = {
        'id':'',
        'userid':'',
        'nick':'',
        'comment':'',
        'refer_nick':'',
        'refer':'',
        'crawler_time':'',
        'time':'',
        'star':'0'
    }
    for comment in comments:
        comment_dict = copy.deepcopy(template_dict)
        cmt = comment.find_element_by_css_selector('.cnt.f-brk')
        nick = cmt.find_element_by_tag_name('a').text
        cmt_con = cmt.text[cmt.text.find(nick)+len(nick)+1:]
        comment_dict['id']=song_id
        comment_dict['nick']=nick
        comment_dict['comment']=cmt_con
        ref = comment.find_elements_by_css_selector('.que.f-brk.f-pr.s-fc3')
        comment_dict['refer_nick']=''
        comment_dict['refer']=''
        if(len(ref)!=0):
            if(len(ref[0].find_elements_by_tag_name('a'))!=0):
                refer_nick = ref[0].find_element_by_tag_name('a').text
                comment_dict['refer_nick']=refer_nick
                comment_dict['refer']=ref[0].text[ref[0].text.find(refer_nick)+len(refer_nick)+1:]
            else:
                # print('评论已删除')
                comment_dict['refer_nick']='0xffff' # 过滤并标记评论删除的情况
        comment_dict['crawler_time'] = time.strftime('%Y/%m/%d %H:%M:%S',time.localtime(time.time()))
        comment_dict['time']=comment.find_element_by_css_selector('.time.s-fc4').text
        star = comment.find_element_by_css_selector('.rp').find_elements_by_tag_name('a')[0].text
        if(len(star)!=0):
            comment_dict['star']=str(star[1:-1])
        user_url = comment.find_element_by_css_selector('.cnt.f-brk').find_element_by_tag_name('a').get_attribute('href')
        user_id = user_url.split('=')[-1]
        comment_dict['userid'] = user_id
        avatarURL = comment.find_element_by_css_selector('.head').find_element_by_tag_name('img').get_attribute('src')
        try:
            saveAvatar(avatarURL,user_id)   # 忽略头像保存错误，由于头像图片删除原因
        except:
            print(nick + ' avatar save happen error')
        comment_list.append(comment_dict)
    return comment_list

# 歌曲解析
def songParser(br,song_id,threadID):
    br.get('https://music.163.com/song?id=' + song_id)
    print('<Thread-'+str(threadID)+'>https://music.163.com/song?id=' + song_id + '页面到达...')
    br.switch_to.frame('contentFrame')  # 跳转到frame中
    time.sleep(1)
    song_name = br.find_element_by_css_selector('.tit').text
    song_author = br.find_elements_by_css_selector('.des.s-fc4')[0].find_element_by_tag_name('span').get_attribute('title')
    saveSongInfo(song_id,song_name,song_author)
    print('<Thread-%d>%s-%s-->评论开始抓取...' % (threadID,song_name, song_author))
    try:
        totalPages = int(br.find_elements_by_class_name('zpgi')[-1].text)
    except:
        print(song_name + ' || ' + song_author + 'ignore')  # 忽略评论页数不足一页的歌曲
        return -2
    for i in range(totalPages): # 总页数可改，仅做测试更改
        print('<Thread-%d>(%s-%s)第<%d/%d>页评论开始抓取...' % (threadID,song_name, song_author,i + 1, totalPages))
        cmts = br.find_elements_by_class_name('itm')
        comment_list = commentsParser(cmts,song_id)
        count[song_id] = count[song_id] + len(comment_list) # 统计每首歌评论抓取的总数
        print('<Thread-%d>(%s-%s)成功抓取第<%d/%d>页%d条评论...' % (threadID,song_name, song_author,i + 1, totalPages, len(comment_list)))
        jsonSaver(comment_list,'./data/comment/'+song_id+'.json',i) # 通过页数做标志位，保证歌曲评论唯一
        scriptClick(br, br.find_element_by_link_text('下一页'))
        time.sleep(2)   # 页面点击缓冲
    print('<Thread-%d>-----%s_%s抓取完毕 | 共计抓取%d条评论-----' % (threadID,song_name,song_author,count[song_id]))

# 获取歌曲id，领取任务
def getSongID():
    if(len(song_ids)!=0):
        tmpID = song_ids[0]
        song_ids.remove(tmpID)
        print('>>>目前还未抓取的歌曲ID:',song_ids)
        print('>>>未抓取歌曲数量:',len(song_ids))
        return tmpID
    else:
        return -1

class CrawlThread(threading.Thread):
    def __init__(self,threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.opt = webdriver.FirefoxOptions()
        self.opt.add_argument('--headless') # 以无窗口形式执行
        self.br = webdriver.Firefox(firefox_options=self.opt)
        print('<Thread-%d>创建成功...' % (self.threadID))
    def run(self):
        print('<Thread-%d>开启...' % (self.threadID))
        while(True): # 采用无限循环方式实现常驻线程，无任务可领时，通过break退出循环
            threadLock.acquire()
            song_id = getSongID()   # 线程获取歌曲id，为避免脏读，采用锁机制
            threadLock.release()
            if(song_id!=-1):
                songParser(self.br,song_id,self.threadID)
            else:   # 当返回为-1时，表示无歌曲评论爬取任务可领，销毁线程
                self.br.quit()
                print('<Thread-%d>销毁...' % (self.threadID))
                break

if __name__ == '__main__':
    song_ids = [
        '186436','186453','26620756','1463165983','417859631',
        '27955654','32507038','415792881','468517654','1374056689',
        '574919767','28059417','569213220','569214250','569200212'
    ]
    count = {}
    for id in song_ids:
        count[id] = 0
    threadLock = threading.Lock()
    ThreadNum = 4   # 定义要创建线程的个数
    Thread_List = []
    for i in range(ThreadNum):
        Thread_List.append(CrawlThread(i))
        Thread_List[-1].start()
    for t in Thread_List:
        t.join()    # 等待所有线程结束后结束主线程
    print('全部抓取完成...')



