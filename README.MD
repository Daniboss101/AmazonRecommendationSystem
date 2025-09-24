# Amazon Product Recommendation System

A hybrid recommendation system built on Amazon product review data (2023) that combines collaborative filtering with content-based approaches to handle data sparsity and cold start problems.

## Overview

This system processes over 1.5M Amazon product reviews across multiple categories to provide personalized recommendations. It addresses the challenges of extreme data sparsity (98.76% sparse user-item matrix) through intelligent fallback strategies and category-based recommendations.

## Key Features

- **Hybrid Architecture**: Combines collaborative filtering with content-based recommendations
- **Smart Fallback System**: Automatically switches strategies based on data availability and confidence scores
- **Category-Based Recommendations**: Leverages product categories for users with limited interaction history
- **Popularity-Based Cold Start**: Handles new users with zero interaction history
- **Large-Scale Data Processing**: Handles millions of reviews with efficient chunked model storage
- **PostgreSQL Integration**: Robust database backend for data persistence

## Dataset Statistics

- **Reviews**: 1,518,464 total interactions
- **Users**: 192,188 unique users (56.5% single reviewers)
- **Products**: 913,986 unique items (76% single-review items)  
- **Categories**: 42 product categories
- **Sparsity**: 99.5% (extreme sparse matrix)
- **Data Coverage**: 2020-2023 Amazon reviews

## System Architecture

### Data Pipeline
1. **Data Ingestion** (`load_review.py`): Streams and processes gzipped Amazon review files
2. **Metadata Processing** (`load_meta.py`): Extracts product metadata and category information
3. **Data Cleaning** (`Data_Cleaning_Preprocessing.ipynb`): Handles duplicates, missing values, and data validation
4. **Data Exploration** ('EDA.ipynb): Explores the data through various charts and manipulations
5. **Model Building** ('model_development.ipynb'): Ensembles two models: Matrix Factorization(Fallback Popularity model) and Content-Based

### Model Components

1. **Collaborative Filtering**: Matrix factorization for users with sufficient interaction history
2. **Category-Based Filtering**: Popularity scoring within user's preferred categories
3. **Popularity-Based**: Overall popular items for cold start scenarios
4. **Confidence Scoring**: Dynamic threshold adjustment based on recommendation quality

## Project Files

This repository contains Jupyter notebooks and Python scripts documenting the complete analysis and model development process:

- `Data_Cleaning_Preprocessing.ipynb`: Data cleaning, validation, and feature engineering
- `EDA.ipynb`: Comprehensive exploratory data analysis with visualizations
- `model_development.ipynb`: Recommendation system implementation and evaluation
- `load_review.py`: Amazon review data ingestion pipeline
- `load_meta.py`: Product metadata processing
- `amazon_df.parquet`: Processed dataset ready for analysis

## Data Insights

### Category Distribution
- **Amazon Home** dominates with 25.1% of products
- **High Gini coefficient** (0.61) indicates uneven distribution
- **Long tail**: 20+ categories have <1% market share each

### User Behavior Patterns  
- **56.5% single reviewers**: Require popularity-based recommendations
- **13% power users** (10+ reviews): Enable collaborative filtering
- **Average 7.9 reviews per user**: Heavily skewed distribution

### Data Sparsity Challenges
- **98.76% matrix sparsity**: Traditional collaborative filtering insufficient
- **96.3% items have <5 reviews**: Content-based methods essential
- **27.3% repeat purchasers**: Indicate user loyalty opportunities

### Quality Metrics
- **Average rating**: 4.2/5.0 across all categories
- **Highest rated**: Collectible Coins (4.56), Digital Music (4.54)
- **Most controversial**: Gift Cards (σ=1.26), Software (σ=1.22)

## Model Performance

### Recommendation Strategy Distribution
- **Collaborative filtering**: ~13% of users (power users)
- **Category-based**: ~31% of users (moderate history)  
- **Popularity-based**: ~56% of users (cold start)

### Key Performance Indicators
- **Confidence threshold**: 0.4 for collaborative filtering switch
- **Category coverage**: 42 categories with specialized recommendations
- **Fallback success**: 100% recommendation coverage (no empty responses)

## Technical Challenges Addressed

1. **Extreme Sparsity**: Hybrid approach with intelligent fallbacks
2. **Cold Start Problem**: Category analysis and popularity recommendations
3. **Data Quality**: Robust cleaning pipeline handling duplicates and missing values
4. **Category Imbalance**: Weighted sampling and category-specific models


## Future Enhancements

- **Deep Learning Models**: Implement neural collaborative filtering
- **Real-time Updates**: Streaming recommendation updates
- **A/B Testing Framework**: Compare recommendation strategies
- **Advanced NLP**: Process review text for sentiment-based recommendations
- **Multi-armed Bandit**: Optimize exploration vs exploitation
- **Cross-category Recommendations**: Leverage category relationships

