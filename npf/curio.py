from curio import run, spawn
import curio_http

async def fetch_one(url):
    async with curio_http.ClientSession() as session:
        response = await session.get(url)
        content = await response.json()
        return response, content


async def main(url_list):
    tasks = []

    for url in url_list:
        task = await spawn(fetch_one(url))
        tasks.append(task)

    for task in tasks:
        response, content = await task.join()

        print('GET %s' % response.url)
        print(content)
        print()


url_list = [
    'http://httpbin.org/delay/1',
    'http://httpbin.org/delay/2',
    'http://httpbin.org/delay/3',
    'http://httpbin.org/delay/4',
]

if __name__ == '__main__':
    run(main(url_list))