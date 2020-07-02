const puppeteer = require('puppeteer');
const lineByLine = require('n-readlines');
const asyncRedis = require("async-redis");

class I {
    constructor(ifname, proxies) {
        this.browsers = [];
        this.client = asyncRedis.createClient();

        (async () => {
            for(let i = 0; i < proxies.length; i++) {
                let args = [`--no-sandbox`, `--proxy-server=${proxies[i]}`, `--ignore-certificate-errors`];
                this.browsers.push(await puppeteer.launch({args: args}));
            }

            this.ids = await this.readIds(ifname);

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
            await this.client.lpush("id_to_username", `${this.ids[i]}\t${username}`);
            await this.client.lpush("processed_user_ids", this.ids[i]);
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

    async readIds(ifname) {
        let list = [];
        let readstream = new lineByLine(ifname);

        let alreadyProcessed = new Set([await this.client.lrange("processed_user_ids", 0, -1)]);
        
        while(true) {
            let line = readstream.next();
            if(!line) break;
            
            line = line.toString("ascii");
            let userID = line.split("\t")[0];

            if(!alreadyProcessed.has(userID)) {
                list.push(userID);
            }
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