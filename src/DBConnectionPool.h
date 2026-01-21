#pragma once

#include <deque>
#include <exception>
#include <libgen.h>
#include <memory>
#include <mutex>
#include <set>
#include <string>

// #define DBCONNECTIONPOOL_LOG

#ifdef DBCONNECTIONPOOL_LOG
#include "ThreadLogger.h"
#endif


struct ConnectionUnavailable : std::exception
{
	char const *what() const throw() override { return "Unable to allocate connection"; };
};

class DBConnection
{

  protected:
	std::string _selectTestingConnection;
	int _connectionId;

  public:
	std::chrono::system_clock::time_point _lastActivity;

  public:
	/*
		DBConnection()
			{
					_selectTestingConnection	= "";
					_connectionId				= -1;
			};
	*/
	DBConnection(const std::string &selectTestingConnection, int connectionId)
	{
		_selectTestingConnection = selectTestingConnection;
		_connectionId = connectionId;
	};
	virtual ~DBConnection() = default;

	int getConnectionId() const { return _connectionId; }

	virtual bool connectionValid() { return true; };
};

class DBConnectionFactory
{

  public:
	virtual ~DBConnectionFactory() = default;
	virtual std::shared_ptr<DBConnection> create(int connectionId) = 0;
};

struct DBConnectionPoolStats
{
	size_t _poolSize;
	size_t _borrowedSize;
};

template <class T> class DBConnectionPool
{

  protected:
	std::shared_ptr<DBConnectionFactory> _factory;
	size_t _poolSize;
	std::deque<std::shared_ptr<DBConnection>> _connectionPool;
	std::set<std::shared_ptr<DBConnection>> _connectionBorrowed;
	std::mutex _connectionPoolMutex;

  public:
	DBConnectionPool(size_t poolSize, const std::shared_ptr<DBConnectionFactory> &factory)
	{
		_poolSize = poolSize;
		_factory = factory;

#ifdef DBCONNECTIONPOOL_LOG
		LOG_TRACE("building DBConnectionPool");
#endif

		int lastConnectionId = 0;

		// Fill the pool
		while (_connectionPool.size() < _poolSize)
		{
// shared_ptr<DBConnection> sqlConnection =
// _factory->create(lastConnectionId++); if (sqlConnection != nullptr)
// 	_connectionPool.push_back(sqlConnection);
#ifdef DBCONNECTIONPOOL_LOG
			LOG_TRACE("Creating connection {}", lastConnectionId);
#endif
			_connectionPool.push_back(_factory->create(lastConnectionId++));
		}
	};

	~DBConnectionPool() = default;

	/**
	 * Borrow
	 *
	 * Borrow a connection for temporary use
	 *
	 * When done, either (a) call unborrow() to return it, or (b) (if it's bad)
	 * just let it go out of scope.  This will cause it to automatically be
	 * replaced.
	 * @retval a shared_ptr to the connection object
	 */
	std::shared_ptr<T> borrow()
	{
#ifdef DBCONNECTIONPOOL_LOG
		LOG_TRACE("Received borrow");
#endif

#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point borrowBegin = chrono::system_clock::now();
		chrono::system_clock::time_point lockerBegin = chrono::system_clock::now();
#endif
		std::lock_guard<std::mutex> locker(_connectionPoolMutex);
#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point lockerEnd = chrono::system_clock::now();
		chrono::system_clock::time_point sizeZeroBegin = chrono::system_clock::now();
#endif

		// Check for a free connection
		if (_connectionPool.empty())
		{
#ifdef DBCONNECTIONPOOL_LOG
			LOG_TRACE("_connectionPool.size is 0, look to recover a borrowed one");
#endif

			// Are there any crashed connections listed as "borrowed"?
			for (std::set<std::shared_ptr<DBConnection>>::iterator it = _connectionBorrowed.begin(); it != _connectionBorrowed.end(); ++it)
			{
				// generally use_count is 2, one because of borrowed
				// set and one because it is used for sql statements.
				// If it is 1, it means this connection has been abandoned
				if ((*it).use_count() == 1)
				{
					// Destroy it and create a new connection
					try
					{
						// If we are able to create a new connection, return it
#ifdef DBCONNECTIONPOOL_LOG
						LOG_TRACE("Creating new connection to replace discarded connection");
#endif

						int connectionId = (*it)->getConnectionId();

						std::shared_ptr<DBConnection> sqlConnection = _factory->create(connectionId);
						if (sqlConnection == nullptr)
						{
							std::string errorMessage = "sqlConnection is null";
#ifdef DBCONNECTIONPOOL_LOG
							LOG_ERROR(errorMessage);
#endif

							throw std::runtime_error(errorMessage);
						}

						_connectionBorrowed.erase(it);
						_connectionBorrowed.insert(sqlConnection);

						sqlConnection->_lastActivity = std::chrono::system_clock::now();

						return static_pointer_cast<T>(sqlConnection);
					}
					catch (std::exception &e)
					{
						std::string errorMessage = "exception";
#ifdef DBCONNECTIONPOOL_LOG
						LOG_ERROR(errorMessage);
#endif

						// Error creating a replacement connection
						throw std::runtime_error(errorMessage);
					}
				}
			}

			std::string errorMessage = "No connection available";
#ifdef DBCONNECTIONPOOL_LOG
			LOG_ERROR(errorMessage);
#endif

			// Nothing available
			throw std::runtime_error(errorMessage);
		}
#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point sizeZeroEnd = chrono::system_clock::now();
		chrono::system_clock::time_point dataStructure_1Begin = chrono::system_clock::now();
#endif

		// Take one off the front
		std::shared_ptr<DBConnection> sqlConnection = _connectionPool.front();
		_connectionPool.pop_front();
		/*
		if (sqlConnection->getConnectionId() == 0)
		{
				_connectionPool.pop_front();
				_connectionPool.push_back(sqlConnection);

				sqlConnection = _connectionPool.front();
		}
		*/
#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point dataStructure_1End = chrono::system_clock::now();
#endif

		if (sqlConnection == nullptr)
		{
			std::string errorMessage = "sqlConnection is null";
#ifdef DBCONNECTIONPOOL_LOG
			LOG_ERROR(errorMessage);
#endif

			throw std::runtime_error(errorMessage);
		}

#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point connectionValidBegin = chrono::system_clock::now();
#endif
		// shared_ptr<T> customSqlConnection =
		// static_pointer_cast<T>(sqlConnection);
		bool connectionValid = sqlConnection->connectionValid();
		if (!connectionValid)
		{
#ifdef DBCONNECTIONPOOL_LOG
			LOG_WARN(
				"sqlConnection is null or is not valid"
				", connectionValid: {}",
				connectionValid
			);
#endif

			int connectionId = sqlConnection->getConnectionId();

			try
			{
				// we will create a new connection. The previous connection
				// will be deleted by the shared_ptr
				sqlConnection = _factory->create(connectionId);
				if (sqlConnection == nullptr)
				{
					// in this scenario we will lose one connection
					// because we removed it from _connectionPool
					// and we will not add it to _connectionBorrowed
					// We will accept that since we were not be able
					// to create a new connection

					std::string errorMessage = "sqlConnection is null";
#ifdef DBCONNECTIONPOOL_LOG
					LOG_ERROR(errorMessage);
#endif

					throw std::runtime_error(errorMessage);
				}
			}
			catch (std::runtime_error &e)
			{
#ifdef DBCONNECTIONPOOL_LOG
				LOG_ERROR(
					"sql connection creation failed"
					", e.what(): {}",
					e.what()
				);
#endif

				// 2022-07-31: in case the create fails, we have to put
				//	the connection again into the pool
				//	Scenario: mysql server is restarted.
				//		In this scenario, if we have a pool of 100 connections,
				//		all the 'create' fail and the pool remain without
				// connections. 		The result is that the the pool of the
				// application (i.e. MMS) 		remain with a pool without
				// connection. To recover, the application 		has to be
				// restarted once the mysql server is running again.
				// So, to avoid that, we will insert the 'non valid' connection again
				// into the pool in order next time the 'create' can be tried again

				_connectionPool.push_back(sqlConnection);

				throw e;
			}
			catch (std::exception &e)
			{
#ifdef DBCONNECTIONPOOL_LOG
				LOG_ERROR(
					"sql connection creation failed"
					", e.what(): {}",
					e.what()
				);
#endif

				// 2022-07-31: in case the create fails, we have to put
				//	the connection again into the pool
				//	Scenario: mysql server is restarted.
				//		In this scenario, if we have a pool of 100 connections,
				//		all the 'create' fail and the pool remain without
				// connections. 		The result is that the the pool of the
				// application (i.e. MMS) 		remain with a pool without
				// connection. To recover, the application 		has to be
				// restarted once the mysql server is running again.
				// So, to avoid that, we will insert the 'non valid' connection again
				// into the pool in order next time the 'create' can be tried again

				_connectionPool.push_back(sqlConnection);

				throw e;
			}
		}
#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point connectionValidEnd = chrono::system_clock::now();
		chrono::system_clock::time_point dataStructure_2Begin = chrono::system_clock::now();
#endif

		_connectionBorrowed.insert(sqlConnection);
#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point dataStructure_2End = chrono::system_clock::now();
#endif

#ifdef DBCONNECTIONPOOL_LOG
		LOG_TRACE(
			"borrow"
			", connectionId: {}",
			sqlConnection->getConnectionId()
		);
#endif
#ifdef DBCONNECTIONPOOL_STATS_LOG
		chrono::system_clock::time_point borrowEnd = chrono::system_clock::now();
		LOG_INFO(
			"borrow elapsed"
			", locker: {}"
			", sizeZero: {}"
			", connectionValid: {}"
			", dataStructure: {}"
			", borrow: {}",
			chrono::duration_cast<chrono::milliseconds>(lockerEnd - lockerBegin).count(),
			chrono::duration_cast<chrono::milliseconds>(sizeZeroEnd - sizeZeroBegin).count(),
			chrono::duration_cast<chrono::milliseconds>(connectionValidEnd - connectionValidBegin).count(),
			chrono::duration_cast<chrono::milliseconds>(dataStructure_1End - dataStructure_1Begin).count() +
				chrono::duration_cast<chrono::milliseconds>(dataStructure_2End - dataStructure_2Begin).count(),
			chrono::duration_cast<chrono::milliseconds>(borrowEnd - borrowBegin).count()
		);
#endif

		sqlConnection->_lastActivity = std::chrono::system_clock::now();

		return static_pointer_cast<T>(sqlConnection);
	};

	/**
	 * Unborrow a connection
	 *
	 * Only call this if you are returning a working connection.  If the
	 * connection was bad, just let it go out of scope (so the connection manager
	 * can replace it).
	 * @param the connection
	 */
	void unborrow(std::shared_ptr<T> sqlConnection)
	{
		if (sqlConnection == nullptr)
		{
			std::string errorMessage = "sqlConnection is null";
#ifdef DBCONNECTIONPOOL_LOG
			LOG_ERROR(errorMessage);
#endif

			throw std::runtime_error(errorMessage);
		}

		std::lock_guard<std::mutex> locker(_connectionPoolMutex);

		// Push onto the pool
		_connectionPool.push_back(static_pointer_cast<DBConnection>(sqlConnection));

		// Unborrow
		_connectionBorrowed.erase(sqlConnection);

#ifdef DBCONNECTIONPOOL_LOG
		LOG_TRACE(
			"unborrow"
			", connectionId: {}",
			sqlConnection->getConnectionId()
		);
#endif
	};

	DBConnectionPoolStats get_stats()
	{
		std::lock_guard<std::mutex> locker(_connectionPoolMutex);

		// Get stats
		DBConnectionPoolStats stats;
		stats._poolSize = _connectionPool.size();
		stats._borrowedSize = _connectionBorrowed.size();

		return stats;
	};
};
