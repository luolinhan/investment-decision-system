"""
研报下载系统 - 研报路由
"""
from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import FileResponse
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
import os
import time

from app.database import get_db
from app.models import Report, Stock
from app.services.collector import ReportCollector


router = APIRouter(prefix="/api/reports", tags=["研报"])


# ==================== 响应模型 ====================

class ReportResponse(BaseModel):
    id: int
    title: str
    stock_code: Optional[str]
    stock_name: Optional[str]
    institution: Optional[str]
    author: Optional[str]
    rating: Optional[str]
    publish_date: Optional[date]
    pdf_url: Optional[str]
    local_pdf_path: Optional[str]
    summary: Optional[str]
    source: Optional[str]

    class Config:
        from_attributes = True


class ReportListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    reports: List[ReportResponse]


class StockResponse(BaseModel):
    id: int
    code: str
    name: str
    market: Optional[str]
    category: Optional[str]
    enabled: bool
    report_count: Optional[int] = 0

    class Config:
        from_attributes = True


class StockListResponse(BaseModel):
    total: int
    stocks: List[StockResponse]


class CollectRequest(BaseModel):
    stock_code: Optional[str] = None
    keyword: Optional[str] = None
    days: int = 30


class CollectResponse(BaseModel):
    success: bool
    message: str
    count: int = 0


# ==================== 研报接口 ====================

@router.get("", response_model=ReportListResponse)
async def list_reports(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    stock_code: Optional[str] = Query(None, description="股票代码筛选"),
    institution: Optional[str] = Query(None, description="机构筛选"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    keyword: Optional[str] = Query(None, description="关键词搜索"),
    has_pdf: Optional[bool] = Query(None, description="是否有PDF"),
    db: AsyncSession = Depends(get_db)
):
    """获取研报列表"""

    # 构建查询
    query = select(Report)
    count_query = select(func.count(Report.id))

    # 筛选条件
    conditions = []

    if stock_code:
        conditions.append(Report.stock_code == stock_code)

    if institution:
        conditions.append(Report.institution.contains(institution))

    if start_date:
        conditions.append(Report.publish_date >= start_date)

    if end_date:
        conditions.append(Report.publish_date <= end_date)

    if keyword:
        conditions.append(
            or_(
                Report.title.contains(keyword),
                Report.stock_name.contains(keyword),
                Report.institution.contains(keyword)
            )
        )

    if has_pdf is not None:
        if has_pdf:
            conditions.append(Report.local_pdf_path.isnot(None))
        else:
            conditions.append(Report.local_pdf_path.is_(None))

    if conditions:
        query = query.where(and_(*conditions))
        count_query = count_query.where(and_(*conditions))

    # 排序和分页
    query = query.order_by(Report.publish_date.desc(), Report.id.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    # 执行查询
    result = await db.execute(query)
    reports = result.scalars().all()

    count_result = await db.execute(count_query)
    total = count_result.scalar()

    return ReportListResponse(
        total=total,
        page=page,
        page_size=page_size,
        reports=[ReportResponse.model_validate(r) for r in reports]
    )


@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(
    report_id: int,
    db: AsyncSession = Depends(get_db)
):
    """获取研报详情"""

    result = await db.execute(
        select(Report).where(Report.id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="研报不存在")

    return ReportResponse.model_validate(report)


@router.get("/{report_id}/pdf")
async def get_report_pdf(
    report_id: int,
    db: AsyncSession = Depends(get_db)
):
    """获取研报PDF - 自动下载并缓存"""

    result = await db.execute(
        select(Report).where(Report.id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="研报不存在")

    # 如果本地PDF存在且有效
    if report.local_pdf_path and os.path.exists(report.local_pdf_path):
        size = os.path.getsize(report.local_pdf_path)
        if size > 5000:  # 有效PDF
            return FileResponse(
                report.local_pdf_path,
                media_type="application/pdf",
                filename=f"report_{report_id}.pdf"
            )

    # 没有本地PDF，尝试使用Chrome下载
    if report.pdf_url:
        import subprocess
        from fastapi.responses import Response

        pdf_dir = "data/pdfs"
        os.makedirs(pdf_dir, exist_ok=True)

        # 使用简单的数字文件名避免编码问题
        filename = f"report_{report_id}.pdf"
        filepath = os.path.join(pdf_dir, filename)
        abs_filepath = os.path.abspath(filepath)

        chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

        try:
            cmd = [
                chrome_path,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                f"--print-to-pdf={abs_filepath}",
                report.pdf_url
            ]

            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if os.path.exists(filepath) and os.path.getsize(filepath) > 5000:
                report.local_pdf_path = filepath
                await db.commit()

                with open(filepath, 'rb') as f:
                    content = f.read()

                return Response(
                    content=content,
                    media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="report_{report_id}.pdf"'}
                )
            else:
                raise HTTPException(status_code=500, detail="PDF生成失败")

        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="PDF下载超时")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"PDF获取错误: {str(e)}")

    raise HTTPException(status_code=404, detail="PDF文件不存在")


@router.get("/{report_id}/pdf-raw")
async def get_report_pdf_raw(
    report_id: int,
    db: AsyncSession = Depends(get_db)
):
    """直接重定向到PDF原链接"""

    result = await db.execute(
        select(Report).where(Report.id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="研报不存在")

    if report.pdf_url:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=report.pdf_url)

    raise HTTPException(status_code=404, detail="PDF链接不存在")


@router.get("/{report_id}/pdf-proxy")
async def get_report_pdf_proxy(
    report_id: int,
    db: AsyncSession = Depends(get_db)
):
    """通过服务器代理获取PDF（绑过安全限制）"""

    result = await db.execute(
        select(Report).where(Report.id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="研报不存在")

    # 如果本地PDF存在且有效
    if report.local_pdf_path and os.path.exists(report.local_pdf_path):
        size = os.path.getsize(report.local_pdf_path)
        if size > 5000:
            return FileResponse(
                report.local_pdf_path,
                media_type="application/pdf",
                filename=f"report_{report_id}.pdf"
            )

    # 没有本地PDF，尝试使用requests代理获取
    if report.pdf_url:
        import requests
        from fastapi.responses import Response

        session = requests.Session()

        # 设置完整的浏览器headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }

        proxy = os.environ.get("HTTP_PROXY", "http://127.0.0.1:7890")

        try:
            # 先访问东方财富主页建立session
            session.get('https://www.eastmoney.com/', headers=headers, proxies={'http': proxy, 'https': proxy}, timeout=10)
            time.sleep(1)

            # 访问数据页面
            headers['Referer'] = 'https://www.eastmoney.com/'
            session.get('https://data.eastmoney.com/report/', headers=headers, proxies={'http': proxy, 'https': proxy}, timeout=10)
            time.sleep(1)

            # 获取PDF
            headers['Accept'] = 'application/pdf,*/*'
            resp = session.get(report.pdf_url, headers=headers, proxies={'http': proxy, 'https': proxy}, timeout=60, allow_redirects=True)

            if resp.status_code == 200 and len(resp.content) > 5000:
                # 保存到本地
                pdf_dir = "data/pdfs"
                os.makedirs(pdf_dir, exist_ok=True)
                filename = f"report_{report_id}.pdf"
                filepath = os.path.join(pdf_dir, filename)

                with open(filepath, 'wb') as f:
                    f.write(resp.content)

                report.local_pdf_path = filepath
                await db.commit()

                return Response(
                    content=resp.content,
                    media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="report_{report_id}.pdf"'}
                )
            else:
                # 如果代理也失败，重定向到原链接让浏览器处理
                from fastapi.responses import RedirectResponse
                return RedirectResponse(url=report.pdf_url)

        except Exception as e:
            print(f"PDF代理获取失败: {e}")
            # 失败时重定向到原链接
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url=report.pdf_url)

    raise HTTPException(status_code=404, detail="PDF文件不存在")


@router.post("/{report_id}/download-pdf")
async def download_report_pdf(
    report_id: int,
    db: AsyncSession = Depends(get_db)
):
    """使用Chrome下载PDF到本地"""

    result = await db.execute(
        select(Report).where(Report.id == report_id)
    )
    report = result.scalar_one_or_none()

    if not report:
        raise HTTPException(status_code=404, detail="研报不存在")

    if report.local_pdf_path and os.path.exists(report.local_pdf_path) and os.path.getsize(report.local_pdf_path) > 5000:
        return {"success": True, "message": "PDF已存在", "path": report.local_pdf_path}

    if not report.pdf_url:
        raise HTTPException(status_code=404, detail="无PDF链接")

    # 使用Chrome打印PDF（绑过安全防护）
    import subprocess

    pdf_dir = "data/pdfs"
    os.makedirs(pdf_dir, exist_ok=True)

    safe_title = "".join(c for c in report.title[:50] if c.isalnum() or c in " -_")
    filename = f"{report.id}_{safe_title}.pdf"
    filepath = os.path.join(pdf_dir, filename)

    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    try:
        cmd = [
            chrome_path,
            "--headless",
            "--disable-gpu",
            "--no-sandbox",
            f"--print-to-pdf={os.path.abspath(filepath)}",
            report.pdf_url
        ]

        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if os.path.exists(filepath) and os.path.getsize(filepath) > 5000:
            report.local_pdf_path = filepath
            await db.commit()
            return {"success": True, "message": "PDF下载成功", "path": filepath, "size": os.path.getsize(filepath)}
        else:
            raise HTTPException(status_code=500, detail=f"PDF生成失败: 文件过小或不存在")

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="PDF下载超时")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")


@router.post("/download-pdfs")
async def download_all_pdfs(
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """批量下载PDF"""

    result = await db.execute(
        select(Report).where(
            Report.pdf_url.isnot(None),
            Report.local_pdf_path.is_(None)
        ).limit(limit)
    )
    reports = result.scalars().all()

    if not reports:
        return {"success": True, "count": 0, "message": "没有需要下载的PDF"}

    import httpx
    proxy = os.environ.get("HTTP_PROXY", "http://127.0.0.1:7890")
    pdf_dir = "data/pdfs"
    os.makedirs(pdf_dir, exist_ok=True)

    downloaded = 0

    async with httpx.AsyncClient(timeout=60, proxy=proxy, follow_redirects=True) as client:
        for report in reports:
            try:
                resp = await client.get(report.pdf_url)
                if resp.status_code == 200:
                    safe_title = "".join(c for c in report.title[:50] if c.isalnum() or c in " -_")
                    filename = f"{report.id}_{safe_title}.pdf"
                    filepath = os.path.join(pdf_dir, filename)

                    with open(filepath, 'wb') as f:
                        f.write(resp.content)

                    report.local_pdf_path = filepath
                    downloaded += 1
            except:
                continue

    await db.commit()
    return {"success": True, "count": downloaded}


@router.post("/collect", response_model=CollectResponse)
async def collect_reports(
    request: CollectRequest,
    db: AsyncSession = Depends(get_db)
):
    """手动触发采集"""

    collector = ReportCollector(db)

    try:
        if request.stock_code:
            count = await collector.collect_by_stock(
                request.stock_code,
                days=request.days,
                download_pdf=True
            )
            return CollectResponse(
                success=True,
                message=f"采集 {request.stock_code} 完成",
                count=count
            )

        else:
            # 采集所有研报
            count = await collector.collect_all_reports(days=request.days)
            return CollectResponse(
                success=True,
                message=f"采集完成，共新增 {count} 篇研报",
                count=count
            )

    except Exception as e:
        return CollectResponse(
            success=False,
            message=f"采集失败: {str(e)}"
        )


# ==================== 股票接口 ====================

@router.get("/stocks/list", response_model=StockListResponse)
async def list_stocks(
    enabled: Optional[bool] = Query(None, description="是否启用"),
    category: Optional[str] = Query(None, description="类别筛选"),
    db: AsyncSession = Depends(get_db)
):
    """获取关注的股票列表"""

    query = select(Stock)
    conditions = []

    if enabled is not None:
        conditions.append(Stock.enabled == enabled)

    if category:
        conditions.append(Stock.category == category)

    if conditions:
        query = query.where(and_(*conditions))

    query = query.order_by(Stock.category, Stock.code)

    result = await db.execute(query)
    stocks = result.scalars().all()

    # 获取每个股票的研报数量
    stock_list = []
    for stock in stocks:
        count_result = await db.execute(
            select(func.count(Report.id)).where(Report.stock_code == stock.code)
        )
        report_count = count_result.scalar() or 0

        stock_list.append(StockResponse(
            id=stock.id,
            code=stock.code,
            name=stock.name,
            market=stock.market,
            category=stock.category,
            enabled=stock.enabled,
            report_count=report_count
        ))

    return StockListResponse(
        total=len(stock_list),
        stocks=stock_list
    )


@router.post("/stocks/add")
async def add_stock(
    code: str = Query(..., description="股票代码"),
    name: str = Query(..., description="股票名称"),
    market: Optional[str] = Query(None, description="市场：HK/A/US"),
    category: Optional[str] = Query(None, description="类别"),
    db: AsyncSession = Depends(get_db)
):
    """添加关注股票"""

    # 检查是否已存在
    result = await db.execute(
        select(Stock).where(Stock.code == code)
    )
    if result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="股票已存在")

    stock = Stock(
        code=code,
        name=name,
        market=market,
        category=category
    )
    db.add(stock)
    await db.commit()

    return {"success": True, "message": "添加成功", "stock_id": stock.id}


@router.delete("/stocks/{stock_id}")
async def delete_stock(
    stock_id: int,
    db: AsyncSession = Depends(get_db)
):
    """删除关注股票"""

    result = await db.execute(
        select(Stock).where(Stock.id == stock_id)
    )
    stock = result.scalar_one_or_none()

    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")

    await db.delete(stock)
    await db.commit()

    return {"success": True, "message": "删除成功"}


@router.put("/stocks/{stock_id}/toggle")
async def toggle_stock(
    stock_id: int,
    db: AsyncSession = Depends(get_db)
):
    """启用/禁用股票"""

    result = await db.execute(
        select(Stock).where(Stock.id == stock_id)
    )
    stock = result.scalar_one_or_none()

    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")

    stock.enabled = not stock.enabled
    await db.commit()

    return {"success": True, "enabled": stock.enabled}


# ==================== 统计接口 ====================

@router.get("/stats/overview")
async def get_stats_overview(db: AsyncSession = Depends(get_db)):
    """获取统计概览"""

    # 总研报数
    total_reports = await db.execute(select(func.count(Report.id)))
    total_reports = total_reports.scalar() or 0

    # 今日新增
    today = date.today()
    today_reports = await db.execute(
        select(func.count(Report.id)).where(Report.publish_date == today)
    )
    today_reports = today_reports.scalar() or 0

    # 本周新增
    from datetime import timedelta
    week_start = today - timedelta(days=today.weekday())
    week_reports = await db.execute(
        select(func.count(Report.id)).where(Report.publish_date >= week_start)
    )
    week_reports = week_reports.scalar() or 0

    # 按机构统计
    institution_stats = await db.execute(
        select(Report.institution, func.count(Report.id).label("count"))
        .where(Report.institution.isnot(None))
        .group_by(Report.institution)
        .order_by(func.count(Report.id).desc())
        .limit(10)
    )
    top_institutions = [
        {"name": row[0], "count": row[1]}
        for row in institution_stats.all()
    ]

    # 按股票统计
    stock_stats = await db.execute(
        select(Report.stock_name, func.count(Report.id).label("count"))
        .where(Report.stock_name.isnot(None))
        .group_by(Report.stock_name)
        .order_by(func.count(Report.id).desc())
        .limit(10)
    )
    top_stocks = [
        {"name": row[0], "count": row[1]}
        for row in stock_stats.all()
    ]

    # 已下载PDF数量
    pdf_count = await db.execute(
        select(func.count(Report.id)).where(Report.local_pdf_path.isnot(None))
    )
    pdf_count = pdf_count.scalar() or 0

    return {
        "total_reports": total_reports,
        "today_reports": today_reports,
        "week_reports": week_reports,
        "pdf_count": pdf_count,
        "top_institutions": top_institutions,
        "top_stocks": top_stocks,
    }


@router.get("/stats/institutions")
async def get_institution_stats(db: AsyncSession = Depends(get_db)):
    """获取机构统计"""

    result = await db.execute(
        select(Report.institution, func.count(Report.id).label("count"))
        .where(Report.institution.isnot(None))
        .group_by(Report.institution)
        .order_by(func.count(Report.id).desc())
        .limit(50)
    )

    return {
        "institutions": [
            {"name": row[0], "count": row[1]}
            for row in result.all()
        ]
    }