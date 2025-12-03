from datetime import datetime
from typing import List, Optional
from sqlalchemy import String, ForeignKey, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Feature(Base):
    __tablename__ = "features"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(500))
    owner: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    
    # Relationship to versions
    versions: Mapped[List["FeatureVersion"]] = relationship(back_populates="feature", cascade="all, delete-orphan")

class FeatureVersion(Base):
    __tablename__ = "feature_versions"

    id: Mapped[int] = mapped_column(primary_key=True)
    feature_id: Mapped[int] = mapped_column(ForeignKey("features.id"))
    version: Mapped[str] = mapped_column(String(20))  # e.g., "v1", "v2"
    path: Mapped[str] = mapped_column(String(500))    # Path to Parquet file
    computed_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    
    # Metadata for lineage and quality
    git_commit_hash: Mapped[Optional[str]] = mapped_column(String(50))
    drift_status: Mapped[str] = mapped_column(String(20), default="ok") # ok, warning, drift
    
    feature: Mapped["Feature"] = relationship(back_populates="versions")

    def __repr__(self):
        return f"<FeatureVersion(name={self.feature.name}, version={self.version})>"