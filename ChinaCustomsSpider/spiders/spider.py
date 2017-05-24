import scrapy
from ..items import ChinacustomsspiderItem
from urllib.parse import urljoin

class CustomshouseSpider(scrapy.Spider):
    name = 'CustomsSpider'
    start_urls = [
        'http://www.customs.gov.cn/publish/portal0/tab49666/'
    ]

    def parse(self, response):
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

        # 获取海关统计月报区域的链接
        monthly_magazine = response.xpath('//div[@id="tmenu_126614"]/table/tr[2]//a/@href').extract()[0]
        if monthly_magazine:
            monthly_magazine_url = urljoin('http://www.customs.gov.cn', str(monthly_magazine))
            self.log('monthly_magazine: %s' % monthly_magazine_url)
            yield scrapy.Request(url=monthly_magazine_url, callback=self.get_monthly_magazine)

        #获取关区统计区域的链接
        custom_district = response.xpath('//div[@id="tmenu_126614"]/table/tr[5]//a/@href').extract()[0]
        if custom_district:
            self.log('custom_district: %s' % custom_district)
            yield scrapy.Request(url=custom_district, callback=self.get_custom_district)

    def get_flash(self, response):
        # 获取统计快讯的链接
        page_list = response.xpath('//li[@class="liebiaoys24"]/span/a/@href').extract()
        for page in page_list:
            flash_url = urljoin('http://www.customs.gov.cn', str(page))
            self.log("flash_url: %s" % flash_url)
            # 将获取到的页面地址传给parse_flash这个函数进行处理
            yield scrapy.Request(url=flash_url, callback=self.parse_flash)

    def remove_unused(self, element):
        #去掉 [' 以及 ']
        element_str = str(element).lstrip("[\'").rstrip("\']")
        return element_str

    def parse_flash(self, response):
        #对统计快讯进行分析
        item = ChinacustomsspiderItem()
        item['keyword'] = '中国海关; 统计; 快讯'
        item['weburl'] = response.url
        title = response.xpath('//div[@class="titTop"]/h1/strong/text()').extract()
        title_str = self.remove_unused(title)
        item['title'] = title_str
        dataupdate = response.xpath('//div[@class="detailTime"]/text()').extract()
        dataupdate_str = self.remove_unused(dataupdate).strip('发布时间：')
        item['dataupdate'] = dataupdate_str
        contents = response.xpath('//*[@id="zoom"]/table/tbody//tr//text()').extract()
        contents_len = len(contents)
        contents_list = []
        for i in range(contents_len):
            contents_str = str(contents[i]).strip('\n\r    \xa0').strip('\u3000')
            if contents_str is not '':
                contents_list.append(contents_str)
            else:
                pass
        item['contents'] = self.remove_unused(str(contents_list))
        yield item

    def get_monthly_magazine(self, response):
        #获取海关月报的链接
        monthly_magazine_urls = response.xpath('//div[@id="ess_ContentPane"]//td//a//@href').extract()
        for monthly_magazine_url in monthly_magazine_urls:
            self.log('monthly_magazine_url: %s' % monthly_magazine_url)
            yield scrapy.Request(url=monthly_magazine_url, callback=self.parse_monthly_magazine)

    def parse_monthly_magazine(self, response):
        #对统计快讯进行分析
        item = ChinacustomsspiderItem()
        item['keyword'] = '中国海关; 统计; 快讯'
        item['weburl'] = response.url
        title = response.xpath('//div[@class="titTop"]/h1/strong/text()').extract()
        title_str = self.remove_unused(title)
        item['title'] = title_str
        dataupdate = response.xpath('//div[@class="detailTime"]/text()').extract()
        dataupdate_str = self.remove_unused(dataupdate).strip('发布时间：')
        year = int(dataupdate_str.split('-')[0])
        item['keyword'] = '中国海关; 统计数据; 月报; %d' % year
        item['dataupdate'] = dataupdate_str
        contents = response.xpath('//*[@id="zoom"]/table/tbody//tr//text()').extract()
        contents_len = len(contents)
        contents_list = []
        for i in range(contents_len):
            contents_str = str(contents[i]).strip('\n\r    \xa0').strip('\u3000')
            if contents_str is not '':
                contents_list.append(contents_str)
            else:
                pass
        item['contents'] = self.remove_unused(str(contents_list))
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
        item = ChinacustomsspiderItem()
        district_pages = response.xpath('div[@id="con_three3"]/ul//table/tbody/tr')
        for district_page in district_pages:
            district_keyword = str(district_page.xpath('/td[1]/text()').extract()[0]).lstrip('.').rstrip('：')
            item['keyword'] = "中国海关; 统计; 关区概况; %s" % district_keyword
            item['title'] = district_page.xpath('/td[1]/a/text()').extract()[0]
            item['dataupdate'] = district_page.xpath('/td[2]/text()').extract()[0]
            district_url_part = response.xpath('/td[1]/a/@href').extract()[0]
            district_url = urljoin('http://www.customs.gov.cn', str(district_url_part))
            yield scrapy.Request(url=district_url, callback=self.parse_custom_district_contents)
            yield item

    def parse_custom_district_contents(self, response):
        #分析关区统计的内容
        item = ChinacustomsspiderItem()
        contents = response('//div[@id="ess_ctr126755_ModuleContent"]/div[2]/div[4]//text()').extract()
        contents_len = len(contents)
        item['contents'] = []
        for i in range(contents_len):
            contents_str = str(contents[i]).strip('\n\r    \xa0').strip('\u3000')
            if contents_str is not '':
                item['contents'].append(contents_str)
            else:
                pass
        yield item