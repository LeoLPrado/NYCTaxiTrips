# Databricks notebook source
spark.sql(
    """
    CREATE DATABASE IF NOT EXISTS nyctaxi_db
    """
)