#pragma once

#include "DBConnectionPool.h"
#include <format>
#include <pqxx/nontransaction>
#include <pqxx/pqxx>
#include <stdexcept>
#include <string>

// #define DBCONNECTIONSPOOL_LOG

class PostgresConnection : public DBConnection
{

  public:
	std::shared_ptr<pqxx::connection> _sqlConnection;

	/*
					PostgresConnection(): DBConnection()
					{
					}
	*/

	PostgresConnection(std::string selectTestingConnection, int connectionId) : DBConnection(selectTestingConnection, connectionId) {}

	~PostgresConnection() override
	{
#ifdef DBCONNECTIONSPOOL_LOG
		LOG_TRACE(
			"sql connection destruct"
			", _connectionId: {}",
			_connectionId
		);
#endif
	};

	bool connectionValid() override
	{
		bool connectionValid = true;

		if (_sqlConnection == nullptr)
		{
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_ERROR(
				"sql connection is null"
				", _connectionId: {}",
				_connectionId
			);
#endif
			connectionValid = false;
		}
		else
		{
			if (!_selectTestingConnection.empty())
			{
				try
				{
#ifdef DBCONNECTIONSPOOL_LOG
					LOG_TRACE(
						"sql connection test"
						", _connectionId: {}"
						", _selectTestingConnection: {}",
						_connectionId, _selectTestingConnection
					);
#endif
					pqxx::nontransaction trans{*_sqlConnection};

					trans.exec(_selectTestingConnection);

					// This doesn't really do anything
					trans.commit();
				}
				catch (pqxx::sql_error const &e)
				{
#ifdef DBCONNECTIONSPOOL_LOG
					LOG_ERROR(
						"sql connection exception"
						", _connectionId: {}"
						", hostname: {}"
						", e.query(): {}"
						", e.what(): {}",
						_connectionId, _sqlConnection->hostname(), e.query(), e.what()
					);
#endif

					connectionValid = false;
				}
				catch (pqxx::broken_connection const &e)
				{
#ifdef DBCONNECTIONSPOOL_LOG
					LOG_WARN(
						"sql connection exception"
						", _connectionId: {}"
						", elapsed since last activity: {} secs"
						", hostname: {}"
						", _selectTestingConnection: {}"
						", e.what(): {}",
						_connectionId, chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - _lastActivity).count(),
						_sqlConnection->hostname(), _selectTestingConnection, e.what()
					);
#endif

					connectionValid = false;
				}
				catch (std::exception &e)
				{
#ifdef DBCONNECTIONSPOOL_LOG
					LOG_WARN(
						"sql connection exception"
						", _connectionId: {}"
						", hostname: {}"
						", _selectTestingConnection: {}"
						", e.what(): {}",
						_connectionId, _sqlConnection->hostname(), _selectTestingConnection, e.what()
					);
#endif

					connectionValid = false;
				}
			}
		}

		return connectionValid;
	}
};

class PostgresConnectionFactory : public DBConnectionFactory
{

  private:
	std::string _dbServer;
	std::string _dbUsername;
	int _dbPort;
	std::string _dbPassword;
	std::string _dbName;
	// bool _reconnect;
	// std::string _defaultCharacterSet;
	std::string _selectTestingConnection;

  public:
	PostgresConnectionFactory(
		const std::string &dbServer, const std::string &dbUsername, int dbPort, const std::string &dbPassword, const std::string &dbName,
		/* bool reconnect, std::string defaultCharacterSet, */
		std::string selectTestingConnection
	)
	{
		_dbServer = dbServer;
		_dbUsername = dbUsername;
		_dbPort = dbPort;
		_dbPassword = dbPassword;
		_dbName = dbName;
		// _reconnect = reconnect;
		// _defaultCharacterSet = defaultCharacterSet;
		_selectTestingConnection = selectTestingConnection;
	};

	// Any exceptions thrown here should be caught elsewhere
	std::shared_ptr<DBConnection> create(int connectionId)
	{
		try
		{
			// reconnect? character set?
			// std::string connectionDetails = "dbname=" + _dbName + " user=" + _dbUsername
			// 	+ " password=" + _dbPassword + " hostaddr=" + _dbServer + "
			// port=5432";
			std::string connectionDetails = std::format("postgresql://{}:{}@{}:{}/{}", _dbUsername, _dbPassword, _dbServer, _dbPort, _dbName);
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_TRACE(
				"sql connection creating..."
				", _dbServer: {}"
				", _dbUsername: {}"
				// ", _dbPassword: {}"
				", _dbName: {}",
				// ", connectionDetails: {}",
				_dbServer, _dbUsername, /* _dbPassword, */ _dbName
			);
			// connectionDetails);
#endif
			std::shared_ptr<pqxx::connection> conn = make_shared<pqxx::connection>(connectionDetails);

			std::shared_ptr<PostgresConnection> postgresConnection = make_shared<PostgresConnection>(_selectTestingConnection, connectionId);
			postgresConnection->_sqlConnection = conn;

			bool connectionValid = postgresConnection->connectionValid();
			if (!connectionValid)
			{
#ifdef DBCONNECTIONSPOOL_LOG
				std::string errorMessage = std::format(
					"just created sql connection is not valid"
					", _connectionId: {}"
					", _dbServer: {}"
					", _dbUsername: {}"
					", _dbName: {}",
					postgresConnection->getConnectionId(), _dbServer, _dbUsername, _dbName
				);
				LOG_ERROR(errorMessage);
#endif

				return nullptr;
			}
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_TRACE(
				"just created sql connection"
				", _connectionId: {}"
				", _dbServer: {}"
				", _dbUsername: {}"
				", _dbName: {}",
				postgresConnection->getConnectionId(), _dbServer, _dbUsername, _dbName
			);
#endif
			postgresConnection->_lastActivity = std::chrono::system_clock::now();

			return static_pointer_cast<DBConnection>(postgresConnection);
		}
		catch (std::exception &e)
		{
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_ERROR(
				"sql connection creation failed"
				", e.what(): {}",
				e.what()
			);
#endif

			throw;
		}
	};
};

class PostgresConnTrans
{
  private:
	std::shared_ptr<DBConnectionPool<PostgresConnection>> _connectionsPool;
	bool _abort;

  public:
	std::unique_ptr<pqxx::transaction_base> transaction;
	std::shared_ptr<PostgresConnection> connection;

	PostgresConnTrans(std::shared_ptr<DBConnectionPool<PostgresConnection>> connectionsPool, bool work)
	{
#ifdef DBCONNECTIONSPOOL_LOG
		LOG_DEBUG(
			"Transaction constructor"
			", work: {}",
			work
		);
#endif
		_abort = false;
		_connectionsPool = connectionsPool;
		connection = _connectionsPool->borrow();
		try
		{
			if (work)
				transaction = std::make_unique<pqxx::work>(*(connection->_sqlConnection));
			else
				transaction = std::make_unique<pqxx::nontransaction>(*(connection->_sqlConnection));
		}
		catch (std::exception &e)
		{
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_ERROR(
				"Transaction failed"
				", connection: {}",
				(connection != nullptr ? connection->getConnectionId() : -1)
			);
#endif
			connectionsPool->unborrow(connection);
			throw;
		}
	}

	void setAbort() { _abort = true; }

	~PostgresConnTrans()
	{
#ifdef DBCONNECTIONSPOOL_LOG
		LOG_DEBUG(
			"Transaction destructor"
			", abort: {}",
			_abort
		);
#endif
		try
		{
			if (_abort)
				transaction->abort();
			else
				transaction->commit();
		}
		catch (std::exception &e)
		{
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_ERROR(
				"Transaction abort/commit failed"
				", abort: {}"
				", connection: {}",
				_abort, (connection != nullptr ? connection->getConnectionId() : -1)
			);
#endif
		}
		if (connection != nullptr)
			_connectionsPool->unborrow(connection);
	}
};

class PostgresTransaction
{
  private:
	bool _abort;

  public:
	std::unique_ptr<pqxx::transaction_base> transaction;
	std::shared_ptr<PostgresConnection> connection;

	PostgresTransaction(std::shared_ptr<PostgresConnection> connection, bool work)
	{
#ifdef DBCONNECTIONSPOOL_LOG
		LOG_DEBUG(
			"Transaction constructor"
			", work: {}",
			work
		);
#endif
		_abort = false;

		this->connection = connection;
		if (work)
			transaction = std::make_unique<pqxx::work>(*(connection->_sqlConnection));
		else
			transaction = std::make_unique<pqxx::nontransaction>(*(connection->_sqlConnection));
	}

	void setAbort() { _abort = true; }

	~PostgresTransaction()
	{
#ifdef DBCONNECTIONSPOOL_LOG
		LOG_DEBUG(
			"Transaction destructor"
			", abort: {}",
			_abort
		);
#endif
		try
		{
			if (_abort)
				transaction->abort();
			else
				transaction->commit();
		}
		catch (std::exception &e)
		{
#ifdef DBCONNECTIONSPOOL_LOG
			LOG_ERROR(
				"Transaction abort/commit failed"
				", abort: {}",
				_abort
			);
#endif
		}
	}
};
