from crawler import Crawler
from builder import Builder
from cryptography.fernet import Fernet
import asyncio

proxy_source = "https://www.sslproxies.org/"

# FUNDS DATA
fn_info = b"gAAAAABhxF-9RFMB5un7tBNQ8ykSnx3ytoaFK5xgYpQDqtcqFK4Uxbb8bn68kKhiUefvGKBveTu7KquFlpl7SP768HIwh" \
                 b"Iu4i_UaIGrrr1VKdba_QQUxc0Q8cdjr3hviAzgQd0OcdhgRBTnebbh6hVUfFXUHOEYt1g=="

fn_historic_data = b"gAAAAABhxF__FHglsVQwD2j_JVwM8R5sC1kcPiVIrmxcZIWuupAUOVwudlqAqR80w_Ni2JzxEh0AybWRGnev-dGqvFlIvfvS" \
             b"f34l3X1sKMu3OOx-3p-fzxkH6N8xhtIRjr_r90cQhbtLEoexRA_mOGpU55uP3SvBfg=="

fn_monthly_data = b"gAAAAABhxGKa-sSlbCz3HF_oRoW7JlrCnTtFkkYd3T9uHr2SHSIcx-VGrAXm4sgy_-_ap1CMLPaZOnxEv1UIVpipAhYIjNrm" \
             b"StBg6hfoVsGgn5R2ko7TirjqXA0r9S4KLHGwENn-OvkQsRO1QuDekCybJka-7Kw9eV54RW-bKMYLN_dZGmUSJ1w="


# VARIABLE INCOME DATA
vi_company_info = b"gAAAAABhw-zLgKO9-d4epq06ulvu-2H9oNdJ4sdZOv5ZRrcdwt6gGjBinhuoaNW5lYfe8Bte-733BUyn9O4" \
             b"b4sddMvNH-MZu5pbBJen0ee9xTbZv0NFAlWw3KHOPINkxJBPuBwV2X_rEOrtuCxkCNid5cpLyhfZrrFQG7UZ" \
             b"C_kV6plnxKPI_GjA="

vi_quarterly_results = b'gAAAAABhxF1kVdtJLgvwc0iUerNlHXhacK3j3uvc4UH8pupX8pm-J0lSYCgy04yPNhu9VGjEecwiuHDo5HGXgAWLoIGWdg0v3vLRbPHmZvqe3IUuEvZs8GuEU3W2NpPwuLf3TkS2uE5lK0pVvzlgBEiuPwcD7PdokQ=='


vi_quarterly_results_year = b'gAAAAABhxF38WpeXGKKXvZmuoADtczjqOL4WI4EWRQ9PKONWf7qdoguHDVbEpGDLnZSr8i2aR4E2t_fKcSnMND6j3i' \
             b'OgIzlO0DMmPVOPD0wtXecfPZ9-0nIQKE42JLJ0e3Xafs3k9QQqvpEQOBLbpNYTDAu51_Hgz2XzFfZfUWcTLjh1jiktKdw='


def decrypt_url(url):
    f = Fernet(b'zBD0XJb_uD0Qn3tDjtt-Zk1MGSh2ZRlg6nTMXkcdH9U=')
    return f.decrypt(url).decode()


async def loader(url):
    url = decrypt_url(url)
    crawler = Crawler(proxy_source=proxy_source, url=url)
    results = Builder(crawler.crawl()).architect
    return results


async def yearly_loader(encrypted_url, year):
    url = f"{decrypt_url(encrypted_url)}{year}.zip"
    crawler = Crawler(proxy_source=proxy_source, url=url)
    results = Builder(crawler.crawl()).architect
    return results


async def monthly_loader(url, year, month):
    url = f"{decrypt_url(url)}{year}{month}.csv"
    crawler = Crawler(proxy_source=proxy_source, url=url)
    results = Builder(crawler.crawl()).architect
    return results


loop = asyncio.get_event_loop()
status = loop.run_until_complete(yearly_loader(vi_quarterly_results_year, 2022))
loop.close()
