from urllib.parse import urljoin
import re
from ..items import ChinacustomsspiderItem
import scrapy


class CustomshouseSpider(scrapy.Spider):
    name = 'CustomsSpider'
    start_urls = [
        'http://www.customs.gov.cn/publish/portal0/tab49666/'
    ]

    def parse(self, response):
        #获取关区统计区域的链接
        custom_district = response.xpath('//div[@id="tmenu_126614"]/table/tr[5]//a/@href').extract()[0]
        if custom_district:
            self.log('custom_district: %s' % custom_district)
            yield scrapy.Request(url=custom_district, callback=self.get_custom_district)
            '''
        # 获取海关统计月报区域的链接
        monthly_magazine = response.xpath('//div[@id="tmenu_126614"]/table/tr[2]//a/@href').extract()[0]
        if monthly_magazine:
            monthly_magazine_url = urljoin('http://www.customs.gov.cn', str(monthly_magazine))
            self.log('monthly_magazine: %s' % monthly_magazine_url)
            yield scrapy.Request(url=monthly_magazine_url, callback=self.get_monthly_magazine)

        #获取下一页的链接
        pages_text = response.xpath('//div[@id="ess_ctr175903_ListC_Info_AspNetPager"]/table/tr/td[1]//text()').extract()
        pages_count = str(pages_text[2]).split('/')[1]
        print(pages_count)
        i = 1
        while i <= int(pages_count):
            print(i)
            page_url = urljoin('http://www.customs.gov.cn', '/publish/portal0/tab49666/module175903/page%d.htm' % i)
            self.log('page_url: %s' % page_url)
            i += 1
            yield scrapy.Request(url=page_url, callback=self.get_flash)
            '''

    def get_flash(self, response):
        # 获取统计快讯的链接
        page_list = response.xpath('//li[@class="liebiaoys24"]/span/a/@href').extract()
        for page in page_list:
            if 'http' in page:
                self.log("flash_url: %s" % str(page))
                yield scrapy.Request(url=str(page), callback=self.parse_flash)
            else:
                flash_url = urljoin('http://www.customs.gov.cn', str(page))
                self.log("flash_url: %s" % flash_url)
                # 将获取到的页面地址传给parse_flash这个函数进行处理
                yield scrapy.Request(url=flash_url, callback=self.parse_flash)

    def remove_unused(self, element):
        p = re.compile('\s+')
        m = re.compile('\\\[^r]{1,2}\d{0,4}')
        n = re.compile('\\\[r]{1}')
        element_1 = re.sub(p, '', str(element))
        element_2 = re.sub(m, '', element_1)
        element_3 = re.sub(n, '', element_2)
        element_str = element_3.lstrip("['").rstrip("']")
        return element_str

    def parse_flash(self, response):
        #对统计快讯进行分析
        item = ChinacustomsspiderItem()
        item['keyword'] = '中国海关; 统计; 快讯'
        item['weburl'] = response.url
        title = response.xpath('//div[@class="titTop"]/h1/strong/text()').extract()
        title_str = self.remove_unused(title)
        u = re.compile('（{1,2}\d{1,2}）{1,2}')
        item['title'] = re.sub(u, '', title_str)
        dataupdate = response.xpath('//div[@class="detailTime"]/text()').extract()
        dataupdate_str = self.remove_unused(dataupdate).strip('发布时间：')
        item['dataupdate'] = dataupdate_str
        item['alltypes'] = ''
        unit = response.xpath('//*[@id="zoom"]/table/tbody/tr[2]//td//*[contains(text(), "单位")]//text()').extract()
        unit_str = self.remove_unused(unit)
        annotation = response.xpath('//*[@id="zoom"]/table/tbody/tr[last()]/td[1]//*[contains(text(), "注")]//text()').extract()
        annotation_str = self.remove_unused(annotation)
        item['description'] = ''
        if unit_str is not '' and unit_str is not '单位':
            item['description'] = unit_str
        else:
            pass
        if annotation_str is not '':
            item['description'] = item['description'] + '\r' + annotation_str
        else:
            pass
        judgment_url = str(response.xpath('*[@id="zoom"]//a/@href').extract()).lstrip("['").rstrip("']")
        judgment_src = str(response.xpath('*[@id="zoom"]//img/@src').extract()).lstrip("['").rstrip("']")
        if judgment_url is '' and judgment_src is '':
            tr = response.xpath('//*[@id="zoom"]/table/tbody//tr')

            list1 = []
            list3 = []
            list4 = []
            list2 = []
            tr_loc = 0
            len_tr = 0
            i = 0

            for x in range(len(tr), len(tr) + 1):
                y = response.xpath('//*[@id="zoom"]/table/tbody//tr//text()').extract()
                if '注' in str(y):
                    len_tr = len(tr) - 1
                else:
                    len_tr = len(tr)
            for y in range(1, 4):
                x = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d]//td//text()' % y).extract()
                judgment_string = ''
                for string in x:
                    string = str(string).strip('\u3000')
                    judgment_string = judgment_string + string
                if '美元' in judgment_string or '人民币' in judgment_string:
                    cool = len(response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+1]//td' % y).extract())
                    for u in range(1, cool + 1):
                        cool2 = str(response.xpath(
                            '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//@colspan' % (y, u)).extract()).lstrip(
                            "['").rstrip("']")
                        str1 = str(response.xpath(
                            '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//text()' % (y, u)).extract()).lstrip(
                            "['").rstrip(
                            "']")
                        str2 = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+2]//td//text()' % y).extract()
                        if cool2 is '':
                            list1.append(str1)
                            list3.append('\u3000')
                        else:
                            list3.append(str1)
                            str2[i] = str1 + '/' + str2[i]
                            str2[i + 1] = str1 + '/' + str2[i + 1]
                            list4.append(str2[i])
                            list4.append(str2[i + 1])
                            if i < len(str2) - 1:
                                i = i + 2
                            else:
                                break
                    tr_loc = y
                    list2 = list1 + list4
                elif judgment_string is '':
                    e = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+1]//td//text()' % y).extract()
                    judgment_string = ''
                    for string in e:
                        string = str(string).strip('\u3000')
                        judgment_string = judgment_string + string
                    if judgment_string is '':
                        pass
                    elif '人民币' in judgment_string or '美元' in judgment_string:
                        pass
                    else:
                        cool = len(response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+1]//td' % y).extract())
                        for u in range(1, cool + 1):
                            cool2 = str(
                                response.xpath(
                                    '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//@colspan' % (y, u)).extract()).lstrip(
                                "['").rstrip("']")
                            str1 = str(response.xpath(
                                '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//text()' % (y, u)).extract()).lstrip(
                                "['").rstrip(
                                "']")
                            str2 = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+2]//td//text()' % y).extract()
                            if cool2 is '':
                                list1.append(str1)
                                list3.append('\u3000')
                            else:
                                list3.append(str1)
                                str2[i] = str1 + '/' + str2[i]
                                str2[i + 1] = str1 + '/' + str2[i + 1]
                                list4.append(str2[i])
                                list4.append(str2[i + 1])
                                if i < len(str2) - 1:
                                    i = i + 2
                                else:
                                    break
                        tr_loc = y
                        list2 = list1 + list4
            item['contents'] = ''
            for x in range(0, len(list2) - 1):
                t = self.remove_unused(list2[x])
                item['contents'] = item['contents'] + '\"' + t + '\"' + ','
            m = self.remove_unused(list2[len(list2) - 1])
            item['contents'] = item['contents'] + '\"' + m + '\"' + '\n'
            for g in range(tr_loc + 3, len_tr + 1):
                c = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d]//td//text()' % g).extract()
                for i in range(0, len(c) - 1):
                    o = self.remove_unused(c[i])
                    if o is '':
                        pass
                    else:
                        item['contents'] = item['contents'] + '\"' + o + '\"' + ','
                q = self.remove_unused(c[len(c) - 1])
                item['contents'] = item['contents'] + '\"' + q + '\"' + '\n'
        elif judgment_src is not '':
            alltypes = []
            type = judgment_src.split('.')[1]
            alltypes.append('{"url":"%s","type":"%s"}' % (judgment_src, type))
            item['alltypes'] = alltypes
        else:
            alltypes = []
            type = judgment_url.split('.')[1]
            alltypes.append('{"url":"%s","type":"%s"}' % (judgment_url, type))
            item['alltypes'] = alltypes
        yield item


    def get_monthly_magazine(self, response):
        #获取海关月报的链接
        monthly_magazine_urls = response.xpath('//div[@id="ess_ContentPane"]//a//@href').extract()
        for monthly_magazine_url in monthly_magazine_urls:
            if 'http' in monthly_magazine_url:
                self.log('monthly_magazine_url: %s' % str(monthly_magazine_url))
                yield scrapy.Request(url=monthly_magazine_url, callback=self.parse_monthly_magazine)
            else:
                monthly_magazine_url = urljoin('http://www.customs.gov.cn', str(monthly_magazine_url))
                self.log('monthly_magazine_url: %s' % monthly_magazine_url)
                yield scrapy.Request(url=monthly_magazine_url, callback=self.parse_monthly_magazine)

    def parse_monthly_magazine(self, response):
        #对统计月报进行分析
        item = ChinacustomsspiderItem()
        item['keyword'] = '中国海关; 统计; 快讯'
        item['weburl'] = response.url
        title = response.xpath('//div[@class="titTop"]/h1/strong/text()').extract()
        title_str = self.remove_unused(title)
        u = re.compile('（{1,2}\d{1,2}）{1,2}')
        item['title'] = re.sub(u, '', title_str)
        dataupdate = response.xpath('//div[@class="detailTime"]/text()').extract()
        dataupdate_str = self.remove_unused(dataupdate).strip('发布时间：')
        year = int(dataupdate_str.split('-')[0])
        item['keyword'] = '中国海关; 统计数据; 月报; %d' % year
        item['dataupdate'] = dataupdate_str
        item['alltypes'] = ''
        unit = response.xpath('//*[@id="zoom"]/table/tbody/tr[1]//td//*[contains(text(), "单位")]//text()').extract()
        unit_str = self.remove_unused(unit)
        annotation = response.xpath(
            '//*[@id="zoom"]/table/tbody/tr[last()]/td[1]//*[contains(text(), "注")]//text()').extract()
        annotation_str = self.remove_unused(annotation)
        item['description'] = ''
        if unit_str is not '' and unit_str is not '单位':
            item['description'] = unit_str
        else:
            pass
        if annotation_str is not '':
            item['description'] = item['description'] + '\r' + annotation_str
        else:
            pass
        judgment_url = str(response.xpath('*[@id="zoom"]//a/@href').extract()).lstrip("['").rstrip("']")
        judgment_src = str(response.xpath('*[@id="zoom"]//img/@src').extract()).lstrip("['").rstrip("']")
        if judgment_url is '' and judgment_src is '':
            tr = response.xpath('//*[@id="zoom"]/table/tbody//tr')

            list1 = []
            list3 = []
            list4 = []
            list2 = []
            tr_loc = 0
            len_tr = 0
            i = 0

            for x in range(len(tr), len(tr) + 1):
                y = response.xpath('//*[@id="zoom"]/table/tbody//tr//text()').extract()
                if '注' in str(y):
                    len_tr = len(tr) - 1
                else:
                    len_tr = len(tr)
            for y in range(1, 4):
                x = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d]//td//text()' % y).extract()
                judgment_string = ''
                for string in x:
                    string = str(string).strip('\u3000')
                    judgment_string = judgment_string + string
                if '美元' in judgment_string or '人民币' in judgment_string:
                    cool = len(response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+1]//td' % y).extract())
                    for u in range(1, cool + 1):
                        cool2 = str(response.xpath(
                            '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//@colspan' % (y, u)).extract()).lstrip(
                            "['").rstrip("']")
                        str1 = str(response.xpath(
                            '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//text()' % (y, u)).extract()).lstrip(
                            "['").rstrip(
                            "']")
                        str2 = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+2]//td//text()' % y).extract()
                        if cool2 is '':
                            list1.append(str1)
                            list3.append('\u3000')
                        else:
                            list3.append(str1)
                            str2[i] = str1 + '/' + str2[i]
                            str2[i + 1] = str1 + '/' + str2[i + 1]
                            list4.append(str2[i])
                            list4.append(str2[i + 1])
                            if i < len(str2) - 1:
                                i = i + 2
                            else:
                                break
                    tr_loc = y
                    list2 = list1 + list4
                elif judgment_string is '':
                    e = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+1]//td//text()' % y).extract()
                    judgment_string = ''
                    for string in e:
                        string = str(string).strip('\u3000')
                        judgment_string = judgment_string + string
                    if judgment_string is '':
                        pass
                    elif '人民币' in judgment_string or '美元' in judgment_string:
                        pass
                    else:
                        cool = len(response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+1]//td' % y).extract())
                        for u in range(1, cool + 1):
                            cool2 = str(
                                response.xpath(
                                    '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//@colspan' % (y, u)).extract()).lstrip(
                                "['").rstrip("']")
                            str1 = str(response.xpath(
                                '//*[@id="zoom"]/table/tbody/tr[%d+1]/td[%d]//text()' % (y, u)).extract()).lstrip(
                                "['").rstrip(
                                "']")
                            str2 = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d+2]//td//text()' % y).extract()
                            if cool2 is '':
                                list1.append(str1)
                                list3.append('\u3000')
                            else:
                                list3.append(str1)
                                str2[i] = str1 + '/' + str2[i]
                                str2[i + 1] = str1 + '/' + str2[i + 1]
                                list4.append(str2[i])
                                list4.append(str2[i + 1])
                                if i < len(str2) - 1:
                                    i = i + 2
                                else:
                                    break
                        tr_loc = y
                        list2 = list1 + list4
            item['contents'] = ''
            for x in range(0, len(list2) - 1):
                t = self.remove_unused(list2[x])
                item['contents'] = item['contents'] + '\"' + t + '\"' + ','
            m = self.remove_unused(list2[len(list2) - 1])
            item['contents'] = item['contents'] + '\"' + m + '\"' + '\n'
            for g in range(tr_loc + 3, len_tr + 1):
                c = response.xpath('//*[@id="zoom"]/table/tbody/tr[%d]//td//text()' % g).extract()
                for i in range(0, len(c) - 1):
                    o = self.remove_unused(c[i])
                    if o is '':
                        pass
                    else:
                        item['contents'] = item['contents'] + '\"' + o + '\"' + ','
                q = self.remove_unused(c[len(c) - 1])
                item['contents'] = item['contents'] + '\"' + q + '\"' + '\n'
        elif judgment_src is not '':
            alltypes = []
            type = judgment_src.split('.')[1]
            alltypes.append('{"url":"%s","type":"%s"}' % (judgment_src, type))
            item['alltypes'] = alltypes
        else:
            alltypes = []
            type = judgment_url.split('.')[1]
            alltypes.append('{"url":"%s","type":"%s"}' % (judgment_url, type))
            item['alltypes'] = alltypes
        yield item

    def get_custom_district(self, response):
        #获取关区统计的链接
        pages_text = response.xpath('//div[@id="ess_ctr126765_ModuleContent"]/div/div[@class="fenyeys"]/div/div[@id="ess_ctr126765_ListC_Info_AspNetPager"]/table/tr/td[1]//text()').extract()
        pages_count = str(pages_text[2]).strip('/')
        print(pages_count)
        i = 1
        while i <= int(pages_count):
            print(i)
            page_url = urljoin('http://www.customs.gov.cn', '/tabid/49629/126765pageidx/%d/Default.aspx' % i)
            self.log('page_url: %s' % page_url)
            i += 1
            yield scrapy.Request(url=page_url, callback=self.parse_custom_district)

    def parse_custom_district(self, response):
        #分析关区统计的关键词上传信息和标题
        district_pages = response.xpath('//*[@id="con_three_3"]/ul//a/@href').extract()
        keywords = response.xpath('//*[@id="con_three_3"]/ul//table/tbody/tr/td[1]/text()').extract()
        for i in range(0, len(district_pages)):
            district_url = urljoin('http://www.customs.gov.cn', district_pages[i])
            keyword = keywords[i]
            print(district_url, keyword)
            yield scrapy.Request(url=district_url, meta={'keyword': keyword}, callback=self.parse_custom_district_contents)

    def parse_custom_district_contents(self, response):
        #分析关区统计的内容
        print('start crawl 2')
        keyword = response.meta['keyword']
        item = ChinacustomsspiderItem()
        keyword = "中国海关; 统计; 关区概况; %s" % keyword
        item['keyword'] = keyword
        item['weburl'] = response.url
        title = response.xpath('//div[@class="titTop"]/h1/strong/text()').extract()
        title_str = self.remove_unused(title)
        u = re.compile('（{1,2}\d{1,2}）{1,2}')
        item['title'] = re.sub(u, '', title_str)
        dataupdate = response.xpath('//div[@class="detailTime"]/text()').extract()
        dataupdate_str = self.remove_unused(dataupdate).strip('发布时间：')
        item['dataupdate'] = dataupdate_str
        item['alltypes'] = ''
        unit = response.xpath('//*[@id="zoom"]/table/tbody/tr[2]//td//*[contains(text(), "单位")]//text()').extract()
        unit_str = self.remove_unused(unit)
        annotation = response.xpath(
            '//*[@id="zoom"]/table/tbody/tr[last()]/td[1]//*[contains(text(), "注")]//text()').extract()
        annotation_str = self.remove_unused(annotation)
        item['description'] = ''
        if unit_str is not '' and unit_str is not '单位':
            item['description'] = unit_str
        else:
            pass
        if annotation_str is not '':
            item['description'] = item['description'] + '\r' + annotation_str
        else:
            pass

        judgment_url = str(response.xpath('*[@id="zoom"]//@href').extract()).lstrip("['").rstrip("']")
        judgment_src = str(response.xpath('*[@id="zoom"]//@src').extract()).lstrip("['").rstrip("']")
        if judgment_url is '' and judgment_src is '':
            tr = response.xpath('//*[@id="zoom"]//tr')

            list1 = []
            list3 = []
            list4 = []
            list2 = []
            tr_loc = 0
            len_tr = 0
            i = 0

            for x in range(len(tr), len(tr) + 1):
                y = response.xpath('//*[@id="zoom"]//*/tr[last()]//td//text()').extract()
                if '注' in str(y):
                    len_tr = len(tr) - 1
                else:
                    len_tr = len(tr)
            for y in range(1, 4):
                x = response.xpath('//*[@id="zoom"]//*/tr[%d]//td//text()' % y).extract()
                judgment_string = ''
                for string in x:
                    string = str(string).strip('\u3000')
                    judgment_string = judgment_string + string
                if '美元' in judgment_string or '人民币' in judgment_string:
                    cool = len(response.xpath('//*[@id="zoom"]//*/tr[%d+1]//td' % y).extract())
                    for u in range(1, cool + 1):
                        cool2 = str(response.xpath(
                            '//*[@id="zoom"]//*/tr[%d+1]/td[%d]//@colspan' % (y, u)).extract()).lstrip(
                            "['").rstrip("']")
                        str1 = str(response.xpath(
                            '//*[@id="zoom"]//*/tr[%d+1]/td[%d]//text()' % (y, u)).extract()).lstrip(
                            "['").rstrip(
                            "']")
                        str2 = response.xpath('//*[@id="zoom"]//*/tr[%d+2]//td//text()' % y).extract()
                        if cool2 is '':
                            list1.append(str1)
                            list3.append('\u3000')
                        else:
                            list3.append(str1)
                            str2[i] = str1 + '/' + str2[i]
                            str2[i + 1] = str1 + '/' + str2[i + 1]
                            list4.append(str2[i])
                            list4.append(str2[i + 1])
                            if i < len(str2) - 1:
                                i = i + 2
                            else:
                                break
                    tr_loc = y
                    list2 = list1 + list4
                elif judgment_string is '':
                    e = response.xpath('//*[@id="zoom"]//*/tr[%d+1]//td//text()' % y).extract()
                    judgment_string = ''
                    for string in e:
                        string = str(string).strip('\u3000')
                        judgment_string = judgment_string + string
                    if judgment_string is '':
                        pass
                    elif '人民币' in judgment_string or '美元' in judgment_string:
                        pass
                    else:
                        cool = len(response.xpath('//*[@id="zoom"]//*/tr[%d+1]//td' % y).extract())
                        for u in range(1, cool + 1):
                            cool2 = str(
                                response.xpath(
                                    '//*[@id="zoom"]//*/tr[%d+1]/td[%d]//@colspan' % (y, u)).extract()).lstrip(
                                "['").rstrip("']")
                            str1 = str(response.xpath(
                                '//*[@id="zoom"]//*/tr[%d+1]/td[%d]//text()' % (y, u)).extract()).lstrip(
                                "['").rstrip(
                                "']")
                            str2 = response.xpath('//*[@id="zoom"]//*/tr[%d+2]//td//text()' % y).extract()
                            if cool2 is '':
                                list1.append(str1)
                                list3.append('\u3000')
                            else:
                                list3.append(str1)
                                str2[i] = str1 + '/' + str2[i]
                                str2[i + 1] = str1 + '/' + str2[i + 1]
                                list4.append(str2[i])
                                list4.append(str2[i + 1])
                                if i < len(str2) - 1:
                                    i = i + 2
                                else:
                                    break
                        tr_loc = y
                        list2 = list1 + list4
            item['contents'] = ''
            for x in range(0, len(list2) - 1):
                t = self.remove_unused(list2[x])
                item['contents'] = item['contents'] + '\"' + t + '\"' + ','
            m = self.remove_unused(list2[len(list2) - 1])
            item['contents'] = item['contents'] + '\"' + m + '\"' + '\n'
            for g in range(tr_loc + 3, len_tr + 1):
                c = response.xpath('//*[@id="zoom"]//*/tr[%d]//td//text()' % g).extract()
                for i in range(0, len(c) - 1):
                    o = self.remove_unused(c[i])
                    if o is '':
                        pass
                    else:
                        item['contents'] = item['contents'] + '\"' + o + '\"' + ','
                item['contents'] = item['contents'] + '\n'
        elif judgment_src is not '':
            alltypes = []
            type = judgment_src.split('.')[1]
            alltypes.append('{"url":"%s","type":"%s"}' % (judgment_src, type))
            item['alltypes'] = alltypes
        else:
            alltypes = []
            type = judgment_url.split('.')[1]
            alltypes.append('{"url":"%s","type":"%s"}' % (judgment_url, type))
            item['alltypes'] = alltypes
        yield item