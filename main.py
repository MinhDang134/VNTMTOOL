from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from src.tools.service import ScraperService
from sqlmodel import Session, create_engine
from src.tools.config import settings
import asyncio

async def main():
    engine = create_engine(settings.DATABASE_URL)
    scraper = ScraperService()
    
    # Schedule daily status check at midnight
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scraper.check_pending_brands,
        CronTrigger(hour=0, minute=0),
        args=[Session(engine)]
    )
    scheduler.start()
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

