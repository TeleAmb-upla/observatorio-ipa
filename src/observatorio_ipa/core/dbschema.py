from sqlalchemy import (
    Engine,
    Column,
    String,
    Integer,
    Text,
    DateTime,
    ForeignKey,
    Index,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, relationship, mapped_column
from datetime import datetime


class Base(DeclarativeBase):
    pass


# Job Status: RUNNING, COMPLETED, FAILED
# Image Export Status: PENDING, RUNNING, COMPLETED, FAILED
# Stats Export Status: PENDING, RUNNING, COMPLETED, FAILED
# Website Update Status: PENDING, COMPLETED, FAILED
# Report Status: SKIP, PENDING, COMPLETED, FAILED


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_status: Mapped[str] = mapped_column(String, nullable=False)
    image_export_status: Mapped[str] = mapped_column(
        String, nullable=False, default="PENDING"
    )
    stats_export_status: Mapped[str] = mapped_column(
        String, nullable=False, default="PENDING"
    )
    website_update_status: Mapped[str] = mapped_column(
        String, nullable=False, default="PENDING"
    )
    report_status: Mapped[str] = mapped_column(
        String, nullable=False, default="PENDING"
    )
    error: Mapped[str | None] = mapped_column(Text)
    timezone: Mapped[str] = mapped_column(String, nullable=False, default="UTC")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    exports: Mapped[list["Export"]] = relationship(
        "Export", back_populates="job", cascade="all, delete"
    )
    modis: Mapped[list["Modis"]] = relationship(
        "Modis", back_populates="job", cascade="all, delete"
    )
    reports: Mapped[list["Report"]] = relationship(
        "Report", back_populates="job", cascade="all, delete"
    )
    website_updates: Mapped[list["WebsiteUpdate"]] = relationship(
        "WebsiteUpdate", back_populates="job", cascade="all, delete"
    )
    file_transfers = relationship(
        "FileTransfer", back_populates="job", cascade="all, delete"
    )

    def __repr__(self) -> str:
        return f"<Job(id={self.id}, job_status={self.job_status}, created_at={self.created_at}, updated_at={self.updated_at})>"


# Export Status: RUNNING, COMPLETED, FAILED, TIMED_OUT
class Export(Base):
    __tablename__ = "exports"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    state: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    target: Mapped[str] = mapped_column(String, nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    task_id: Mapped[str | None] = mapped_column(String, nullable=True)
    task_status: Mapped[str] = mapped_column(String, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_check_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    poll_interval_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship("Job", back_populates="exports")

    __table_args__ = (
        Index("idx_exports_job_id", "job_id"),
        Index("idx_exports_due", "state", "next_check_at"),
        Index("idx_exports_lease", "lease_until"),
    )

    def __repr__(self) -> str:
        return f"<Export(id={self.id}, job_id={self.job_id}, state={self.state}, type={self.type}, name={self.name}, created_at={self.created_at}, updated_at={self.updated_at})>"


class Modis(Base):
    __tablename__ = "modis"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    collection: Mapped[str] = mapped_column(String, nullable=False)
    images: Mapped[int] = mapped_column(Integer, nullable=False)
    last_image: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    job: Mapped["Job"] = relationship("Job", back_populates="modis")

    __table_args__ = (Index("idx_modis_job_id", "job_id"),)

    def __repr__(self) -> str:
        return f"<Modis(id={self.id}, job_id={self.job_id}, name={self.name}, collection={self.collection}, images={self.images}, last_image={self.last_image}, updated_at={self.updated_at})>"


class Report(Base):
    __tablename__ = "reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    job: Mapped["Job"] = relationship("Job", back_populates="reports")

    __table_args__ = (Index("idx_reports_job_id", "job_id"),)

    def __repr__(self) -> str:
        return f"<Report(id={self.id}, job_id={self.job_id}, status={self.status}, attempts={self.attempts}, last_error={self.last_error}, updated_at={self.updated_at})>"


class WebsiteUpdate(Base):
    __tablename__ = "website_updates"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING")
    pull_request_id: Mapped[str | None] = mapped_column(String, nullable=True)
    pull_request_url: Mapped[str | None] = mapped_column(String, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    job: Mapped["Job"] = relationship("Job", back_populates="website_updates")

    __table_args__ = (Index("idx_websites_job_id", "job_id"),)

    def __repr__(self) -> str:
        return f"<WebsiteUpdate(id={self.id}, job_id={self.job_id}, status={self.status}, pull_request_id={self.pull_request_id}, pull_request_url={self.pull_request_url}, attempts={self.attempts}, last_error={self.last_error}, updated_at={self.updated_at})>"


# File Transfer Status: MOVED, NOT_MOVED, ROLLED_BACK
class FileTransfer(Base):
    __tablename__ = "file_transfers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        String, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    export_id: Mapped[str] = mapped_column(
        String, ForeignKey("exports.id", ondelete="CASCADE"), nullable=False
    )
    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    destination_path: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    job: Mapped["Job"] = relationship("Job", back_populates="file_transfers")
    export: Mapped["Export"] = relationship("Export")

    __table_args__ = (
        Index("idx_file_transfers_job_id", "job_id"),
        Index("idx_file_transfers_export_id", "export_id"),
    )

    def __repr__(self) -> str:
        return f"<FileTransfer(id={self.id}, job_id={self.job_id}, export_id={self.export_id}, source_path={self.source_path}, destination_path={self.destination_path}, status={self.status}, updated_at={self.updated_at})>"


def create_db_schema(db_engine: Engine) -> None:
    # if path:
    #     db_path = Path(path).expanduser().resolve()
    #     if db_path.suffix == "":
    #         db_path = db_path / DEFAULT_DB_NAME
    # else:
    #     db_path = (DEFAULT_DB_PATH / DEFAULT_DB_NAME).resolve()

    Base.metadata.create_all(db_engine)
    print(f"Database schema created/updated at {db_engine.url}")
