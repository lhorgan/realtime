const puppeteer = require('puppeteer');
const lineByLine = require('n-readlines');

const asyncRedis = require("async-redis");
const client = asyncRedis.createClient();

class I {
    constructor(ifname, proxies) {
        this.browsers = [];

        (async () => {
            for(let i = 0; i < proxies.length; i++) {
                let args = [`--no-sandbox`, `--proxy-server=${proxies[i]}`, `--ignore-certificate-errors`];
                this.browsers.push(await puppeteer.launch({args: args}));
            }

            this.ids = this.readIds(ifname);
            await this.processIds();

            for(let i = 0; i < proxies.length; i++) {
                this.browsers[i].close();
            }
        })();
    }

    async processIds() {
        for(let i = 0; i < this.ids.length; i++) {
            let page = await this.browsers[i % this.browsers.length].newPage();
            await page.goto(`https://twitter.com/i/user/${this.ids[i]}`, {"waitUntil" : "networkidle0"});
            let url = await page.url();
            let username = url.split("/").pop();
            console.log(username);
            page.close();
            //await this.rest(parseInt(20000 + Math.random() * 20000));
        }
    }

    async rest(ms) {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve();
            }, ms);
        });
    }

    readIds(ifname) {
        let list = [];
        let readstream = new lineByLine(ifname);
        
        while(true) {
            let line = readstream.next();
            if(!line) break;
            line = line.toString("ascii");
            list.push(line.split("\t")[0]);
        }
    
        return list;
    }
}

let proxyList = ["35.245.138.206:3128",
                "35.188.147.108:3128",
                "35.222.86.237:3128",
                "35.239.236.224:3128",
                "35.192.135.179:3128"];

let e = new I("/home/luke/Documents/lazer/id_to_username/latest_sinceid_file_high_active.tsv", proxyList);